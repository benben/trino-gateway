import io.trino.jdbc.TrinoDriver;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class NativeJdbc {
    private static final String[] GATEWAYS = required("TX_GATEWAY_URLS").split(",");
    private static final String[] BACKENDS = System.getenv().getOrDefault("TX_REAL_BACKEND_NAMES", "real-blue,real-green").split(",");
    private static final String[] BACKEND_URLS = required("TX_REAL_BACKEND_URLS").split(",");
    private static final String GROUP = System.getenv().getOrDefault("TX_REAL_ROUTING_GROUP", "real-transaction-test");
    private static final String[] NODE_IDS = new String[2];
    private static final String TRUST_STORE = required("TX_TRUST_STORE");
    private static final String STORE_PASSWORD = readSecret("TX_TRUST_STORE_PASSWORD_FILE");
    private static HttpClient admin;

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing " + name);
        }
        return value;
    }

    private static String readSecret(String variable) {
        try {
            return Files.readString(Path.of(required(variable))).strip();
        }
        catch (Exception failure) {
            throw new IllegalArgumentException("Cannot read private file from " + variable);
        }
    }

    private static Connection connect(int gateway) throws Exception {
        URI endpoint = URI.create(GATEWAYS[gateway]);
        if (!endpoint.getScheme().equals("https") || endpoint.getRawUserInfo() != null || endpoint.getRawQuery() != null) {
            throw new IllegalArgumentException("A credential-free HTTPS Gateway URL is required");
        }
        Properties properties = new Properties();
        properties.setProperty("user", required("TX_TRINO_USER"));
        properties.setProperty("password", required("TX_TRINO_PASSWORD"));
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "FULL");
        properties.setProperty("SSLTrustStorePath", TRUST_STORE);
        properties.setProperty("SSLTrustStorePassword", STORE_PASSWORD);
        properties.setProperty("SSLTrustStoreType", "PKCS12");
        properties.setProperty("extraHeaders", "X-Trino-Routing-Group:" + GROUP);
        properties.setProperty("timeout", "30s");
        return new TrinoDriver().connect("jdbc:trino://" + endpoint.getRawAuthority() + "/tpch/tiny", properties);
    }

    private static String scalar(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            try (ResultSet result = statement.executeQuery(sql)) {
                if (!result.next()) {
                    throw new AssertionError("Query returned no row");
                }
                String value = result.getString(1);
                if (result.next()) {
                    throw new AssertionError("Query returned more than one row");
                }
                return value;
            }
        }
    }

    private static void activate(int target) throws Exception {
        String token = System.getenv("TX_ADMIN_TOKEN");
        if (token != null) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(GATEWAYS[0] + "/gateway/transactions/cutover"))
                    .timeout(Duration.ofSeconds(15)).header("Authorization", "Bearer " + token).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"routingGroup\":\"" + GROUP + "\",\"backendName\":\"" + BACKENDS[target] + "\"}")).build();
            int status = admin.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
            if (status != 200) {
                throw new AssertionError("Transaction-aware cutover failed with HTTP " + status);
            }
            awaitCoordinator(NODE_IDS[target]);
            return;
        }
        for (String gateway : GATEWAYS) {
            for (int backend = 0; backend < BACKENDS.length; backend++) {
                String action = backend == target ? "activate" : "deactivate";
                HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(gateway + "/gateway/backend/" + action + "/" + BACKENDS[backend]))
                        .timeout(Duration.ofSeconds(15)).POST(HttpRequest.BodyPublishers.noBody());
                int status = admin.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                if (status != 200) {
                    throw new AssertionError("Backend administration failed with HTTP " + status);
                }
            }
        }
        awaitCoordinator(NODE_IDS[target]);
    }

    private static String coordinator(Connection connection) throws Exception {
        return scalar(connection, "SELECT node_id FROM system.runtime.nodes WHERE coordinator");
    }

    private static void awaitCoordinator(String expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        for (int index = 0; index < GATEWAYS.length; index++) {
            while (true) {
                try (Connection fresh = connect(index)) {
                    if (coordinator(fresh).equals(expected)) {
                        break;
                    }
                }
                catch (SQLException failure) {
                    if (!failure.getMessage().matches("(?s).*(500|502|503|504).*")) {
                        throw failure;
                    }
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("New JDBC connections did not reach the selected coordinator");
                }
                Thread.sleep(200);
            }
        }
    }

    public static void main(String[] arguments) throws Exception {
        if (!"yes".equals(System.getenv("TX_ALLOW_FIXTURE_MUTATION")) || GATEWAYS.length < 2 || BACKENDS.length != 2 || BACKEND_URLS.length != 2) {
            throw new IllegalArgumentException("Explicit disposable-fixture authorization, two Gateways, and two backends are required");
        }
        boolean baseline = arguments.length == 1 && arguments[0].equals("--baseline");
        if (arguments.length > 0 && !baseline) {
            throw new IllegalArgumentException("Only --baseline is supported");
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(Path.of(TRUST_STORE))) {
            store.load(input, STORE_PASSWORD.toCharArray());
        }
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(null, factory.getTrustManagers(), null);
        admin = HttpClient.newBuilder().sslContext(tls).connectTimeout(Duration.ofSeconds(10)).build();
        for (String name : new String[] {GROUP, BACKENDS[0], BACKENDS[1]}) {
            if (!name.matches("[A-Za-z0-9_-]+")) {
                throw new IllegalArgumentException("Invalid fixture group or backend name");
            }
        }
        for (int index = 0; index < BACKEND_URLS.length; index++) {
            URI info = URI.create(BACKEND_URLS[index] + "/v1/info");
            if (!info.getScheme().equals("https")) {
                throw new IllegalArgumentException("Backend identity URLs must use HTTPS");
            }
            HttpResponse<String> response = admin.send(HttpRequest.newBuilder(info).timeout(Duration.ofSeconds(15)).build(), HttpResponse.BodyHandlers.ofString());
            var node = Pattern.compile("\"nodeId\"\\s*:\\s*\"([A-Za-z0-9._-]+)\"").matcher(response.body());
            if (response.statusCode() != 200 || !node.find()) {
                throw new AssertionError("Backend identity was not available");
            }
            NODE_IDS[index] = node.group(1);
        }
        if (NODE_IDS[0].equals(NODE_IDS[1])) {
            throw new AssertionError("Two distinct backend identities are required");
        }
        try {
            for (int gateway = 0; gateway < GATEWAYS.length; gateway++) {
                activate(0);
                try (Connection connection = connect(gateway)) {
                    if (!scalar(connection, "SELECT count(*) FROM nation").equals("25")) {
                        throw new AssertionError("Native JDBC autocommit result mismatch");
                    }
                    connection.setReadOnly(true);
                    connection.setAutoCommit(false);
                    String owner = coordinator(connection);
                    if (!baseline) {
                        activate(1);
                    }
                    if (!coordinator(connection).equals(owner) || !scalar(connection, "SELECT count(*) FROM nation").equals("25")) {
                        throw new AssertionError("Native JDBC transaction changed coordinator or result");
                    }
                    connection.commit();
                    scalar(connection, "SELECT count(*) FROM nation");
                    connection.rollback();
                    System.out.println("PASS native JDBC gateway " + (gateway + 1) + (baseline ? " baseline" : " transaction cutover"));
                }
            }
        }
        finally {
            activate(0);
        }
    }
}
