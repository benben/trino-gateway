/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.gateway.ha.router;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.gateway.ha.config.PrincipalRoutingConfiguration;
import io.trino.gateway.ha.router.schema.RoutingSelectorResponse;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.DataSize.ofBytes;
import static io.trino.gateway.ha.transaction.TransactionIdentity.error;
import static io.trino.gateway.ha.transaction.TransactionIdentity.singleHeader;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Selects compute from a credential-free control-plane snapshot.
 * Basic usernames are routing hints; coordinators still authenticate passwords and authorize sessions.
 */
public final class PrincipalRoutingGroupSelector
        implements RoutingGroupSelector
{
    private static final Logger log = Logger.get(PrincipalRoutingGroupSelector.class);
    private static final int MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final PrincipalRoutingConfiguration configuration;
    private final LongSupplier clock;
    private volatile Snapshot snapshot;
    private ScheduledExecutorService refreshExecutor;

    private record Snapshot(Map<String, String> routes, long fetchedAt) {}

    PrincipalRoutingGroupSelector(PrincipalRoutingConfiguration configuration, LongSupplier clock)
    {
        this.configuration = configuration;
        this.clock = clock;
    }

    public PrincipalRoutingGroupSelector(HttpClient httpClient, PrincipalRoutingConfiguration configuration)
    {
        this(configuration, System::nanoTime);
        configuration.validate();
        refreshExecutor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("principal-routing-refresh").factory());
        long interval = SECONDS.toMillis(configuration.getRefreshIntervalSeconds());
        var _ = refreshExecutor.scheduleWithFixedDelay(() -> refresh(httpClient), ThreadLocalRandom.current().nextLong(interval), interval, MILLISECONDS);
    }

    @Override
    public boolean isAuthoritative()
    {
        return true;
    }

    @Override
    public RoutingSelectorResponse findRoutingDestination(HttpServletRequest request)
    {
        String principal = basicPrincipal(request);
        Snapshot current = snapshot;
        if (current == null || clock.getAsLong() - current.fetchedAt() >= SECONDS.toNanos(configuration.getMaxStaleSeconds())) {
            throw error(503, "Warehouse routing is temporarily unavailable");
        }
        String group = current.routes().get(principal);
        if (group == null) {
            throw error(403, "Warehouse is not available for Trino routing");
        }
        return new RoutingSelectorResponse(group);
    }

    void installSnapshot(String body)
    {
        long fetchedAt = clock.getAsLong();
        try {
            if (body.getBytes(UTF_8).length > MAX_RESPONSE_BYTES) {
                throw new IllegalArgumentException("Routing snapshot exceeds the response limit");
            }
            JsonNode root = JSON.readTree(body);
            if (root == null || !root.isObject() || root.size() != 1 || !root.has("routes") || !root.get("routes").isArray() || root.get("routes").size() > configuration.getMaxEntries()) {
                throw new IllegalArgumentException("Invalid routing snapshot envelope");
            }
            Map<String, String> routes = new HashMap<>();
            for (JsonNode route : root.get("routes")) {
                if (!route.isObject() || route.size() != 2 || !route.path("principal").isTextual() || !route.path("routingGroup").isTextual()) {
                    throw new IllegalArgumentException("Invalid routing snapshot entry");
                }
                String principal = route.get("principal").textValue();
                String group = route.get("routingGroup").textValue();
                if (principal.isBlank() || principal.length() > 1024 || principal.indexOf(':') >= 0 || principal.chars().anyMatch(character -> character < 32 || character == 127) || !group.matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")) {
                    throw new IllegalArgumentException("Invalid routing snapshot identity");
                }
                if (routes.putIfAbsent(principal, group) != null) {
                    throw new IllegalArgumentException("Routing snapshot contains duplicate principals");
                }
            }
            snapshot = new Snapshot(Map.copyOf(routes), fetchedAt);
        }
        catch (IOException failure) {
            throw new IllegalArgumentException("Invalid routing snapshot JSON");
        }
    }

    void refresh(HttpClient httpClient)
    {
        try {
            Request request = prepareGet()
                    .setUri(URI.create(configuration.getUrl()))
                    .setHeader("X-Duckgres-Internal-Secret", configuration.getToken())
                    .setHeader("Accept", "application/json")
                    .setFollowRedirects(false)
                    .setRequestTimeout(new Duration(configuration.getRequestTimeoutMillis(), MILLISECONDS))
                    .setMaxResponseContentLength(ofBytes(MAX_RESPONSE_BYTES))
                    .build();
            String body = httpClient.execute(request, new ResponseHandler<String, RuntimeException>()
            {
                @Override
                public String handleException(Request ignored, Exception failure)
                {
                    throw new IllegalStateException("Routing snapshot request failed");
                }

                @Override
                public String handle(Request ignored, Response response)
                {
                    if (response.getStatusCode() != 200) {
                        throw new IllegalStateException("Routing snapshot request was rejected");
                    }
                    try {
                        byte[] content = response.getInputStream().readNBytes(MAX_RESPONSE_BYTES + 1);
                        if (content.length > MAX_RESPONSE_BYTES) {
                            throw new IllegalStateException("Routing snapshot exceeds the response limit");
                        }
                        return UTF_8.newDecoder().decode(ByteBuffer.wrap(content)).toString();
                    }
                    catch (IOException failure) {
                        throw new IllegalStateException("Cannot read routing snapshot");
                    }
                }
            });
            installSnapshot(body);
        }
        catch (RuntimeException failure) {
            // Do not log endpoint details, response data or service credentials.
            log.warn("Principal routing snapshot refresh failed; the previous snapshot retains its original expiry");
        }
    }

    private static String basicPrincipal(HttpServletRequest request)
    {
        String authorization = singleHeader(request, "Authorization").orElseThrow(PrincipalRoutingGroupSelector::authenticationRequired);
        if (authorization.length() > 8192 || !authorization.regionMatches(true, 0, "Basic ", 0, 6)) {
            throw authenticationRequired();
        }
        String decoded;
        try {
            decoded = UTF_8.newDecoder().decode(ByteBuffer.wrap(Base64.getDecoder().decode(authorization.substring(6)))).toString();
        }
        catch (IllegalArgumentException | CharacterCodingException failure) {
            throw authenticationRequired();
        }
        int separator = decoded.indexOf(':');
        if (separator < 1 || separator > 1024 || decoded.substring(0, separator).isBlank() || decoded.chars().anyMatch(character -> character < 32 || character == 127)) {
            throw authenticationRequired();
        }
        return decoded.substring(0, separator);
    }

    private static WebApplicationException authenticationRequired()
    {
        return new WebApplicationException(jakarta.ws.rs.core.Response.status(401)
                .header("WWW-Authenticate", "Basic realm=\"Trino\"")
                .type("text/plain").entity("Warehouse routing requires Basic authorization").build());
    }

    @PreDestroy
    public void close()
    {
        if (refreshExecutor != null) {
            refreshExecutor.shutdownNow();
        }
    }
}
