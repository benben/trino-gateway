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
package io.trino.gateway.ha.transaction;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.gateway.ha.clustermonitor.ForMonitor;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.config.TransactionAwarenessConfiguration;
import io.trino.gateway.ha.handler.RoutingTargetHandler;
import io.trino.gateway.ha.handler.schema.RoutingDestination;
import io.trino.gateway.ha.handler.schema.RoutingTargetResponse;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import io.trino.gateway.ha.router.schema.RoutingSelectorResponse;
import io.trino.gateway.ha.transaction.TransactionStore.Admission;
import io.trino.gateway.ha.transaction.TransactionStore.BackendRef;
import io.trino.gateway.ha.transaction.TransactionStore.DrainStatus;
import io.trino.gateway.ha.transaction.TransactionStore.QueryBinding;
import io.trino.gateway.ha.transaction.TransactionStore.ResponseObservation;
import io.trino.gateway.ha.transaction.TransactionStore.StoreException;
import io.trino.gateway.proxyserver.ProxyResponseHandler.ProxyResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.ws.rs.WebApplicationException;
import org.jdbi.v3.core.Jdbi;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.gateway.ha.handler.ProxyUtils.buildUriWithNewCluster;
import static io.trino.gateway.ha.handler.ProxyUtils.extractQueryIdIfPresent;
import static io.trino.gateway.ha.transaction.TransactionIdentity.canonicalTransactionId;
import static io.trino.gateway.ha.transaction.TransactionIdentity.error;
import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class TransactionAwarenessService
{
    private static final String ADMISSION_ATTRIBUTE = TransactionAwarenessService.class.getName() + ".admission";
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final TransactionAwarenessConfiguration config;
    private final TransactionStore store;
    private final TransactionIdentity identity;
    private final GatewayBackendManager backendManager;
    private final HttpClient httpClient;
    private final List<String> statementPaths;
    private final RoutingGroupSelector routingGroupSelector;
    private final String defaultRoutingGroup;

    @Inject
    public TransactionAwarenessService(HaGatewayConfiguration configuration, Jdbi jdbi, GatewayBackendManager backendManager, @ForMonitor HttpClient httpClient, RoutingGroupSelector routingGroupSelector)
    {
        config = configuration.getTransactionAwareness();
        config.validate(configuration.getDataStore());
        store = new TransactionStore(jdbi);
        identity = config.isEnabled() ? new TransactionIdentity(config.getIdentityKey()) : null;
        this.backendManager = backendManager;
        this.httpClient = httpClient;
        statementPaths = configuration.getStatementPaths();
        this.routingGroupSelector = routingGroupSelector;
        defaultRoutingGroup = configuration.getRouting().getDefaultRoutingGroup();
    }

    public boolean isEnabled()
    {
        return config.isEnabled();
    }

    public RoutingTargetResponse resolve(HttpServletRequest request, Supplier<RoutingTargetResponse> ordinaryRouting, Function<RoutingSelectorResponse, RoutingTargetResponse> selectBackend)
    {
        boolean submission = request.getMethod().equals("POST") && statementPaths.contains(request.getRequestURI());
        boolean continuation = !request.getMethod().equals("POST") && (statementPaths.stream().anyMatch(path -> request.getRequestURI().startsWith(path + "/")) || request.getRequestURI().startsWith("/v1/query/"));
        if (isEnabled() && request.getMethod().equals("POST") && !submission) {
            throw error(400, "Transaction-aware query submission requires an exact configured statement path");
        }
        if (isEnabled() && (request.getMethod().equals("PUT") || (continuation && !List.of("GET", "HEAD", "DELETE").contains(request.getMethod())))) {
            throw error(405, "This proxy method is not supported by transaction-aware routing");
        }
        if (!isEnabled() || (!submission && !continuation)) {
            return ordinaryRouting.get();
        }
        return guarded(() -> {
            Optional<String> transaction = TransactionIdentity.transactionId(request);
            Admission admission;
            HttpServletRequest forwardedRequest = request;
            if (submission) {
                String owner = identity.owner(request);
                if (transaction.isPresent()) {
                    admission = store.admitTransaction(transaction.orElseThrow(), owner);
                }
                else {
                    RoutingSelectorResponse selection = routingGroupSelector.findRoutingDestination(request);
                    if (!selection.externalHeaders().isEmpty()) {
                        throw error(400, "Transaction-aware routing does not support external request-header rewrites");
                    }
                    String group = selection.routingGroup() == null || selection.routingGroup().isEmpty() ? defaultRoutingGroup : selection.routingGroup();
                    Optional<String> override = store.getRoute(group);
                    if (override.isPresent()) {
                        admission = store.admitNew(override.orElseThrow(), owner, group);
                        forwardedRequest = RoutingTargetHandler.withRoutingHeaders(request, selection.externalHeaders());
                    }
                    else {
                        RoutingTargetResponse selected = selectBackend.apply(selection);
                        ProxyBackendConfiguration candidate = backendManager.getAllBackends().stream()
                                .filter(backend -> backend.getProxyTo().equals(selected.routingDestination().clusterHost()))
                                .filter(backend -> backend.getRoutingGroup().equals(group))
                                .findFirst().orElseThrow(() -> error(503, "No backend belongs to the selected routing group"));
                        if (store.getBackend(candidate.getName()).isEmpty()) {
                            ensureBackend(candidate.getName());
                        }
                        admission = store.admitNew(candidate.getName(), owner, group);
                        forwardedRequest = selected.modifiedRequest();
                    }
                }
            }
            else {
                String queryId = extractQueryIdIfPresent(request.getRequestURI(), null, statementPaths)
                        .orElseThrow(() -> error(400, "Invalid query continuation path"));
                QueryBinding query = store.getQuery(queryId).orElseThrow(() -> error(404, "Unknown query identifier"));
                identity.validateContinuation(request, query.ownerHash());
                admission = store.admitQuery(queryId, Optional.of(query.ownerHash()), transaction);
            }
            try {
                verifyProcess(admission.backend());
            }
            catch (RuntimeException e) {
                store.rejectAdmission(admission.id());
                throw e;
            }
            forwardedRequest = inlineResults(forwardedRequest);
            forwardedRequest.setAttribute(ADMISSION_ATTRIBUTE, admission);
            BackendRef backend = admission.backend();
            return new RoutingTargetResponse(new RoutingDestination(backend.routingGroup(), backend.url(), buildUriWithNewCluster(backend.url(), forwardedRequest), backend.externalUrl()), forwardedRequest);
        });
    }

    public ProxyResponse recordResponse(HttpServletRequest request, ProxyResponse response)
    {
        Admission admission = admission(request);
        if (admission == null) {
            return response;
        }
        return guarded(() -> {
            if ((response.statusCode() == 401 || response.statusCode() == 403) &&
                    responseHeader(response, "X-Trino-Started-Transaction-Id").isEmpty() && responseHeader(response, "X-Trino-Clear-Transaction-Id").isEmpty() &&
                    !response.body().stripLeading().startsWith("{")) {
                store.rejectAdmission(admission.id());
                return response;
            }
            if (response.statusCode() != 200) {
                store.markUncertain(admission.id());
                return response;
            }
            if (request.getMethod().equals("HEAD")) {
                if (responseHeader(response, "X-Trino-Started-Transaction-Id").isPresent() || responseHeader(response, "X-Trino-Clear-Transaction-Id").isPresent()) {
                    throw error(502, "Backend returned transaction lifecycle headers on a heartbeat");
                }
                store.recordResponse(admission.id(), new ResponseObservation(admission.queryId(), null, false, false, config.getTerminalRetentionSeconds()));
                return response;
            }
            JsonNode body;
            try {
                body = JSON.readTree(response.body());
            }
            catch (IOException e) {
                throw error(502, "Backend returned malformed query results");
            }
            if (request.getRequestURI().startsWith("/v1/query/") && request.getMethod().equals("GET")) {
                if (responseHeader(response, "X-Trino-Started-Transaction-Id").isPresent() || responseHeader(response, "X-Trino-Clear-Transaction-Id").isPresent()) {
                    throw error(502, "Backend returned transaction lifecycle headers on query metadata");
                }
                if (body == null || !body.isObject() || !body.path("queryId").asText().equals(admission.queryId())) {
                    throw error(502, "Backend returned invalid query metadata");
                }
                store.recordResponse(admission.id(), new ResponseObservation(admission.queryId(), null, false, false, config.getTerminalRetentionSeconds()));
                return response;
            }
            if (body == null || !body.isObject() || !body.path("id").isTextual() || !body.path("stats").isObject()) {
                throw error(502, "Backend returned query results without a valid identifier");
            }
            if (body.hasNonNull("data") && !body.path("data").isArray()) {
                throw error(502, "Transaction-aware routing currently requires inline query results");
            }
            String queryId = body.path("id").asText();
            if (!queryId.matches("[0-9]+_[0-9]+_[0-9]+_[a-zA-Z0-9]+") || !queryId.endsWith("_" + admission.backend().coordinatorId())) {
                throw error(502, "Backend query identity does not match its registered process");
            }
            Optional<String> started = responseHeader(response, "X-Trino-Started-Transaction-Id");
            if (started.isPresent()) {
                try {
                    started = Optional.of(canonicalTransactionId(started.orElseThrow()));
                }
                catch (WebApplicationException e) {
                    throw error(502, "Backend returned a malformed transaction identifier");
                }
            }
            boolean clear = responseHeader(response, "X-Trino-Clear-Transaction-Id").isPresent();
            if (clear && started.isPresent()) {
                throw error(502, "Backend returned contradictory transaction lifecycle headers");
            }
            boolean terminal = !body.hasNonNull("nextUri");
            if (!terminal) {
                if (!body.path("nextUri").isTextual()) {
                    throw error(502, "Backend returned an invalid continuation");
                }
                URI next;
                try {
                    next = URI.create(body.path("nextUri").asText());
                }
                catch (IllegalArgumentException e) {
                    throw error(502, "Backend returned an invalid continuation");
                }
                if (!List.of("http", "https").contains(next.getScheme()) || !extractQueryIdIfPresent(next.getPath(), null, statementPaths).equals(Optional.of(queryId))) {
                    throw error(502, "Backend continuation does not match its query");
                }
            }
            store.recordResponse(admission.id(), new ResponseObservation(queryId, started.orElse(null), clear, terminal, config.getTerminalRetentionSeconds()));
            if (started.isPresent() && !store.getTransaction(started.orElseThrow()).orElseThrow().state().equals("OPEN")) {
                throw error(409, "A completed transaction cannot be started again by a replayed response");
            }
            return response;
        });
    }

    public void requestFailed(HttpServletRequest request)
    {
        Admission admission = admission(request);
        if (admission != null) {
            try {
                store.markUncertain(admission.id());
            }
            catch (RuntimeException ignored) {
                // The durable admission remains a drain blocker when the database is unavailable.
            }
        }
    }

    public void requestRejectedBeforeDispatch(HttpServletRequest request)
    {
        Admission admission = admission(request);
        if (admission != null) {
            guarded(() -> {
                store.rejectAdmission(admission.id());
                return null;
            });
        }
    }

    public void requireAdmin(HttpServletRequest request)
    {
        if (!isEnabled()) {
            throw error(404, "Transaction awareness is disabled");
        }
        String authorization = TransactionIdentity.singleHeader(request, "Authorization").orElse("");
        if (!MessageDigest.isEqual(authorization.getBytes(UTF_8), ("Bearer " + config.getAdminToken()).getBytes(UTF_8))) {
            throw error(403, "Transaction administration requires its configured bearer token");
        }
    }

    public Map<String, Object> transaction(String transactionId)
    {
        return guarded(() -> {
            TransactionStore.TransactionBinding binding = store.getTransaction(canonicalTransactionId(transactionId))
                    .orElseThrow(() -> error(404, "Unknown transaction identifier"));
            return Map.of("transactionId", binding.transactionId(), "state", binding.state(), "backendName", binding.backend().name(), "backendUrl", binding.backend().url(), "incarnation", binding.backend().incarnation());
        });
    }

    public Map<String, Object> drain(String name, boolean begin)
    {
        return guarded(() -> {
            if (begin && store.getBackend(name).isEmpty()) {
                ensureBackend(name);
            }
            return status(begin ? store.beginDrain(name) : store.drainStatus(name));
        });
    }

    public Map<String, Object> seal(String name, long generation)
    {
        return guarded(() -> {
            verifyProcess(store.getBackend(name).orElseThrow(() -> error(404, "Unknown backend")));
            return status(store.seal(name, generation));
        });
    }

    public Map<String, Object> resume(String name, long generation)
    {
        return guarded(() -> {
            ensureBackend(name);
            return status(store.resume(name, generation));
        });
    }

    public Map<String, Object> cutover(String routingGroup, String backendName)
    {
        return guarded(() -> {
            BackendRef backend = ensureBackend(backendName);
            if (!backend.routingGroup().equals(routingGroup)) {
                throw error(409, "Destination does not belong to the routing group");
            }
            return Map.of("generation", store.setRoute(routingGroup, backendName), "backendName", backendName, "routingGroup", routingGroup);
        });
    }

    public Map<String, Object> reincarnate(String name, UUID incarnation, long generation)
    {
        return guarded(() -> {
            ProxyBackendConfiguration backend = backendManager.getBackendByName(name).orElseThrow(() -> error(404, "Unknown backend"));
            JsonNode info = processInfo(backend.getProxyTo());
            BackendRef proposed = new BackendRef(name, UUID.randomUUID(), backend.getProxyTo(), backend.getExternalUrl(), backend.getRoutingGroup(), info.path("nodeId").asText(), info.path("coordinatorId").asText());
            return status(store.reincarnate(name, incarnation, generation, proposed));
        });
    }

    public Map<String, Object> clearRoute(String routingGroup)
    {
        return guarded(() -> Map.of("generation", store.clearRoute(routingGroup)));
    }

    private BackendRef ensureBackend(String name)
    {
        ProxyBackendConfiguration backend = backendManager.getBackendByName(name).orElseThrow(() -> error(404, "Unknown backend"));
        JsonNode info = processInfo(backend.getProxyTo());
        return store.ensureBackend(name, backend.getProxyTo(), backend.getExternalUrl() == null ? backend.getProxyTo() : backend.getExternalUrl(), backend.getRoutingGroup(), info.path("nodeId").asText(), info.path("coordinatorId").asText());
    }

    private void verifyProcess(BackendRef backend)
    {
        JsonNode info = processInfo(backend.url());
        if (!backend.nodeId().equals(info.path("nodeId").asText()) || !backend.coordinatorId().equals(info.path("coordinatorId").asText())) {
            throw error(409, "The original coordinator process is no longer available");
        }
    }

    private JsonNode processInfo(String backend)
    {
        StringResponse response = httpClient.execute(prepareGet().setUri(URI.create(backend + "/v1/info")).setFollowRedirects(false).build(), createStringResponseHandler());
        try {
            JsonNode info = JSON.readTree(response.getBody());
            if (response.getStatusCode() != 200 || info == null || !info.path("coordinator").asBoolean() || info.path("starting").asBoolean(true) || info.path("nodeId").asText().isBlank() || !info.path("coordinatorId").asText().matches("[a-zA-Z0-9]{5}")) {
                throw error(503, "Backend must expose a ready Trino coordinator process identity");
            }
            return info;
        }
        catch (IOException e) {
            throw error(503, "Backend process identity is unavailable");
        }
    }

    private static Map<String, Object> status(DrainStatus status)
    {
        return Map.ofEntries(Map.entry("name", status.name()),
                Map.entry("incarnation", status.incarnation()),
                Map.entry("state", status.state()),
                Map.entry("generation", status.generation()),
                Map.entry("pendingRequests", status.pendingRequests()),
                Map.entry("openTransactions", status.openTransactions()),
                Map.entry("activeQueries", status.activeQueries()),
                Map.entry("readyToSeal", status.readyToSeal()),
                Map.entry("drained", status.drained()),
                Map.entry("sealed", status.state().equals("SEALED")),
                Map.entry("acceptingNewQueries", status.state().equals("ACTIVE")));
    }

    private static Optional<String> responseHeader(ProxyResponse response, String name)
    {
        List<String> values = response.headers().get(HeaderName.of(name));
        if (values.size() > 1 || (values.size() == 1 && values.getFirst().isEmpty())) {
            throw error(502, "Backend returned ambiguous transaction lifecycle headers");
        }
        return values.stream().findFirst();
    }

    private static Admission admission(HttpServletRequest request)
    {
        return (Admission) request.getAttribute(ADMISSION_ATTRIBUTE);
    }

    static HttpServletRequest inlineResults(HttpServletRequest request)
    {
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public String getHeader(String name)
            {
                return name.equalsIgnoreCase("X-Trino-Query-Data-Encoding") ? null : super.getHeader(name);
            }

            @Override
            public Enumeration<String> getHeaders(String name)
            {
                return name.equalsIgnoreCase("X-Trino-Query-Data-Encoding") ? Collections.emptyEnumeration() : super.getHeaders(name);
            }

            @Override
            public Enumeration<String> getHeaderNames()
            {
                return Collections.enumeration(Collections.list(super.getHeaderNames()).stream()
                        .filter(name -> !name.equalsIgnoreCase("X-Trino-Query-Data-Encoding"))
                        .toList());
            }
        };
    }

    private static <T> T guarded(Supplier<T> action)
    {
        try {
            return action.get();
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (StoreException e) {
            int status = switch (e.code()) {
                case NOT_FOUND -> 404;
                case OWNER_MISMATCH -> 403;
                case NOT_ACTIVE -> 503;
                default -> 409;
            };
            throw error(status, "Transaction routing state rejected the operation: " + e.code());
        }
        catch (RuntimeException e) {
            throw error(503, "Transaction routing state is unavailable; the request was not reassigned");
        }
    }
}
