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

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.handler.schema.RoutingDestination;
import io.trino.gateway.ha.handler.schema.RoutingTargetResponse;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import io.trino.gateway.ha.router.schema.RoutingSelectorResponse;
import io.trino.gateway.ha.transaction.TransactionStore.Admission;
import io.trino.gateway.ha.transaction.TransactionStore.BackendRef;
import io.trino.gateway.ha.transaction.TransactionStore.ErrorCode;
import io.trino.gateway.ha.transaction.TransactionStore.QueryBinding;
import io.trino.gateway.ha.transaction.TransactionStore.ResponseObservation;
import io.trino.gateway.ha.transaction.TransactionStore.StoreException;
import io.trino.gateway.ha.transaction.TransactionStore.TransactionBinding;
import io.trino.gateway.proxyserver.ProxyResponseHandler.ProxyResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestTransactionAwarenessService
{
    private static final String QUERY = "20260909_120000_00001_abcde";
    private static final String TRANSACTION = "00000000-0000-0000-0000-000000000001";
    private static final String CONTINUATION = "/v1/statement/executing/" + QUERY + "/capability/1";
    private static final String RESULTS = "{\"id\":\"" + QUERY + "\",\"stats\":{\"state\":\"FINISHED\"}}";
    private static final BackendRef BACKEND = new BackendRef("blue", UUID.randomUUID(), "http://blue.example.test", "http://blue.example.test", "group", "node", "abcde");
    private MockedConstruction<TransactionStore> construction;
    private TransactionStore store;
    private TransactionAwarenessService service;
    private HaGatewayConfiguration configuration;
    private GatewayBackendManager backendManager;
    private HttpClient httpClient;
    private RoutingGroupSelector selector;

    @BeforeEach
    void setup()
    {
        construction = mockConstruction(TransactionStore.class);
        configuration = new HaGatewayConfiguration();
        DataStoreConfiguration database = new DataStoreConfiguration();
        database.setJdbcUrl("jdbc:postgresql://localhost/unused_unit_test_database");
        configuration.setDataStore(database);
        configuration.getTransactionAwareness().setEnabled(true);
        configuration.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-adapter-tests");
        configuration.getTransactionAwareness().setAdminToken("synthetic-admin-key-for-adapter-tests");
        backendManager = mock(GatewayBackendManager.class);
        httpClient = mock(HttpClient.class);
        selector = mock(RoutingGroupSelector.class);
        service = new TransactionAwarenessService(configuration, mock(Jdbi.class), backendManager, httpClient, selector);
        store = construction.constructed().getFirst();
    }

    @AfterEach
    void closeConstruction()
    {
        construction.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{", "{}", "[]", "null", "{\"id\":1,\"stats\":{}}", "{\"id\":\"20260909_120000_00001_abcde\"}", "{\"id\":\"20260909_120000_00001_abcde\",\"stats\":{}} {}"})
    void malformedOrTruncatedResultsCannotSettleAdmission(String body)
    {
        HttpServletRequest request = admitted("POST", "/v1/statement", null, null);
        expectStatus(502, () -> service.recordResponse(request, response(200, body)));
        verifyNoInteractions(store);
        service.requestFailed(request);
        verify(store).markUncertain(any(UUID.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"state\":null}", "{\"state\":1}", "{\"state\":\"RUNNING\"}", "{\"state\":\"QUEUED\"}", "{\"state\":\"FINISHING\"}", "{\"state\":\"UNKNOWN\"}"})
    void absentContinuationRequiresTerminalQueryState(String stats)
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        String body = "{\"id\":\"" + QUERY + "\",\"stats\":" + stats + "}";
        expectStatus(502, () -> service.recordResponse(request, response(200, body, "X-Trino-Clear-Transaction-Id", "true")));
        verifyNoInteractions(store);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FINISHED", "FAILED"})
    void validTerminalStatesSettleQueryWithoutInferringTransactionClosure(String state)
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        String body = "{\"id\":\"" + QUERY + "\",\"stats\":{\"state\":\"" + state + "\"}}";
        service.recordResponse(request, response(200, body));
        verify(store).recordResponse(admission(request).id(), new ResponseObservation(QUERY, null, false, true, 120));
    }

    @Test
    void unsupportedEncodedResponseCannotSettleAdmission()
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, null);
        String body = RESULTS.substring(0, RESULTS.length() - 1) + ",\"data\":{\"encoding\":\"json+zstd\",\"segments\":[]}}";
        expectStatus(502, () -> service.recordResponse(request, response(200, body)));
        verifyNoInteractions(store);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"id\":\"20260909_120000_00001_abcde\",\"stats\":{\"state\":\"FINISHED\"},\"nextUri\":\"http://blue.example.test/v1/statement/executing/20260909_120000_00001_abcde/capability/1\",\"nextUri\":null}",
            "{\"id\":\"20260909_120000_00002_abcde\",\"id\":\"20260909_120000_00001_abcde\",\"stats\":{\"state\":\"FINISHED\"}}",
            "{\"id\":\"20260909_120000_00001_abcde\",\"stats\":null,\"stats\":{\"state\":\"FINISHED\"}}",
            "{\"id\":\"20260909_120000_00001_abcde\",\"stats\":{\"state\":\"FINISHED\"},\"partialCancelUri\":\"http://blue.example.test/v1/statement/executing/partialCancel/20260909_120000_00001_abcde/1/capability/1\",\"partialCancelUri\":null}",
    })
    void duplicateResultFieldsCannotSettleAdmissionOrRecordCapabilities(String body)
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        assertSoftly(softly -> {
            softly.assertThatThrownBy(() -> service.recordResponse(request, response(200, body, "X-Trino-Clear-Transaction-Id", "true")))
                    .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(502));
            softly.assertThatCode(() -> verifyNoInteractions(store)).doesNotThrowAnyException();
        });
    }

    @Test
    void heartbeatSettlesOnlyItsAdmissionWithoutTerminalObservation()
    {
        HttpServletRequest request = admitted("HEAD", CONTINUATION, QUERY, null);
        Admission admission = admission(request);
        ProxyResponse response = response(200, "");
        assertThat(service.recordResponse(request, response)).isSameAs(response);
        verify(store).recordResponse(admission.id(), new ResponseObservation(QUERY, null, false, false, 120));
    }

    @ParameterizedTest
    @ValueSource(strings = {"X-Trino-Started-Transaction-Id", "X-Trino-Clear-Transaction-Id"})
    void heartbeatCannotForwardUnrecordedLifecycleHeaders(String header)
    {
        HttpServletRequest request = admitted("HEAD", CONTINUATION, QUERY, TRANSACTION);
        expectStatus(502, () -> service.recordResponse(request, response(200, "", header, TRANSACTION)));
        verifyNoInteractions(store);
    }

    @Test
    void metadataCannotDeclareResultDeliveryComplete()
    {
        HttpServletRequest request = admitted("GET", "/v1/query/" + QUERY, QUERY, null);
        Admission admission = admission(request);
        ProxyResponse response = response(200, "{\"queryId\":\"" + QUERY + "\",\"state\":\"FINISHED\"}");
        assertThat(service.recordResponse(request, response)).isSameAs(response);
        verify(store).recordResponse(admission.id(), new ResponseObservation(QUERY, null, false, false, 120));
    }

    @ParameterizedTest
    @ValueSource(strings = {"X-Trino-Started-Transaction-Id", "X-Trino-Clear-Transaction-Id"})
    void metadataCannotForwardUnrecordedLifecycleHeaders(String header)
    {
        HttpServletRequest request = admitted("GET", "/v1/query/" + QUERY, QUERY, TRANSACTION);
        expectStatus(502, () -> service.recordResponse(request, response(200, "{\"queryId\":\"" + QUERY + "\"}", header, TRANSACTION)));
        verifyNoInteractions(store);
    }

    @Test
    void mismatchedMetadataCannotSettleAnotherQuery()
    {
        HttpServletRequest request = admitted("GET", "/v1/query/" + QUERY, QUERY, null);
        expectStatus(502, () -> service.recordResponse(request, response(200, "{\"queryId\":\"20260909_120000_00002_abcde\"}")));
        verifyNoInteractions(store);
    }

    @Test
    void replayedStartCannotReExposeClosedTransaction()
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, null);
        Admission admission = admission(request);
        when(store.getTransaction(TRANSACTION)).thenReturn(Optional.of(new TransactionBinding(TRANSACTION, "owner", BACKEND, QUERY, "CLOSED")));
        expectStatus(409, () -> service.recordResponse(request, response(200, RESULTS, "X-Trino-Started-Transaction-Id", TRANSACTION)));
        verify(store).recordResponse(admission.id(), new ResponseObservation(QUERY, TRANSACTION, false, true, 120));
    }

    @Test
    void clearHeaderUsesProtocolPresenceRatherThanBooleanParsing()
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        Admission admission = admission(request);
        service.recordResponse(request, response(200, RESULTS, "X-Trino-Clear-Transaction-Id", "false"));
        verify(store).recordResponse(admission.id(), new ResponseObservation(QUERY, null, true, true, 120));
    }

    @ParameterizedTest
    @ValueSource(strings = {"X-Trino-Started-Transaction-Id", "X-Trino-Clear-Transaction-Id"})
    void duplicateOrEmptyLifecycleHeadersCannotSettleAdmission(String header)
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        expectStatus(502, () -> service.recordResponse(request, response(200, RESULTS, header, TRANSACTION, header, TRANSACTION)));
        expectStatus(502, () -> service.recordResponse(request, response(200, RESULTS, header, "")));
        verifyNoInteractions(store);
    }

    @Test
    void malformedAndContradictoryLifecycleHeadersCannotSettleAdmission()
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        expectStatus(502, () -> service.recordResponse(request, response(200, RESULTS, "X-Trino-Started-Transaction-Id", "not-a-uuid")));
        expectStatus(502, () -> service.recordResponse(request, response(200, RESULTS, "X-Trino-Started-Transaction-Id", TRANSACTION, "X-Trino-Clear-Transaction-Id", "true")));
        verifyNoInteractions(store);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 409, 429, 500, 503})
    void unprovenNonSuccessResponsesRemainUncertain(int status)
    {
        HttpServletRequest request = admitted("DELETE", CONTINUATION, QUERY, null);
        Admission admission = admission(request);
        ProxyResponse response = response(status, "upstream outcome unavailable");
        assertThat(service.recordResponse(request, response)).isSameAs(response);
        verify(store).markUncertain(admission.id());
        verify(store, never()).rejectAdmission(any());
        verify(store, never()).recordResponse(any(), any());
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403})
    void authenticationRejectionWithoutLifecycleSignalSettlesOnlyAdmission(int status)
    {
        HttpServletRequest request = admitted("POST", "/v1/statement", null, null);
        Admission admission = admission(request);
        service.recordResponse(request, response(status, "authentication rejected"));
        verify(store).rejectAdmission(admission.id());
        verify(store, never()).recordResponse(any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void rejectedCapabilitySettlesTransportWithoutCompletingQuery(String method)
    {
        HttpServletRequest request = admitted(method, CONTINUATION, QUERY, TRANSACTION);
        Admission admission = admission(request);
        ProxyResponse rejected = response(404, "Query not found");
        assertThat(service.recordResponse(request, rejected)).isSameAs(rejected);
        verify(store).rejectAdmission(admission.id());
        verify(store, never()).markUncertain(any());
        verify(store, never()).recordResponse(any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {CONTINUATION, "/v1/statement/executing/partialCancel/" + QUERY + "/1/capability/1", "/v1/query/" + QUERY})
    void acknowledgedCancellationSettlesOnlyTransport(String path)
    {
        HttpServletRequest request = admitted("DELETE", path, QUERY, TRANSACTION);
        Admission admission = admission(request);
        ProxyResponse acknowledged = response(204, "");
        assertThat(service.recordResponse(request, acknowledged)).isSameAs(acknowledged);
        verify(store).rejectAdmission(admission.id());
        verify(store, never()).markUncertain(any());
        verify(store, never()).recordResponse(any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"POST", "GET", "HEAD"})
    void nonCancellationNoContentRemainsUncertain(String method)
    {
        HttpServletRequest request = admitted(method, method.equals("POST") ? "/v1/statement" : CONTINUATION, method.equals("POST") ? null : QUERY, TRANSACTION);
        service.recordResponse(request, response(204, ""));
        verify(store).markUncertain(admission(request).id());
        verify(store, never()).rejectAdmission(any());
        verify(store, never()).recordResponse(any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"X-Trino-Started-Transaction-Id", "X-Trino-Clear-Transaction-Id"})
    void cancellationAcknowledgementWithLifecycleSignalRemainsUncertain(String header)
    {
        HttpServletRequest request = admitted("DELETE", CONTINUATION, QUERY, TRANSACTION);
        service.recordResponse(request, response(204, "", header, TRANSACTION));
        verify(store).markUncertain(admission(request).id());
        verify(store, never()).rejectAdmission(any());
        verify(store, never()).recordResponse(any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void rejectedCapabilityWithLifecycleSignalRemainsUncertain(String method)
    {
        HttpServletRequest request = admitted(method, CONTINUATION, QUERY, TRANSACTION);
        Admission admission = admission(request);
        service.recordResponse(request, response(404, "Query not found", "X-Trino-Started-Transaction-Id", TRANSACTION));
        verify(store).markUncertain(admission.id());
        verify(store, never()).rejectAdmission(any());
        verify(store, never()).recordResponse(any(), any());
    }

    @Test
    void statementNotFoundRemainsUncertain()
    {
        HttpServletRequest request = admitted("POST", "/v1/statement", null, null);
        Admission admission = admission(request);
        service.recordResponse(request, response(404, "Query not found"));
        verify(store).markUncertain(admission.id());
        verify(store, never()).rejectAdmission(any());
    }

    @Test
    void persistenceFailureCannotExposeTheBackendResponse()
    {
        HttpServletRequest request = admitted("POST", "/v1/statement", null, null);
        Admission admission = admission(request);
        doThrow(new IllegalStateException("synthetic database outage")).when(store).recordResponse(any(), any());
        expectStatus(503, () -> service.recordResponse(request, response(200, RESULTS)));
        service.requestFailed(request);
        verify(store).markUncertain(admission.id());
        verifyNoInteractions(selector, backendManager, httpClient);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/v1/statement/", "/v1/statement;parameter", "/v1/./statement", "/v1/%73tatement", "/ui/api/statement"})
    void noncanonicalSubmissionCannotBypassAdmission(String path)
    {
        HttpServletRequest request = request("POST", path, Map.of());
        expectStatus(400, () -> service.resolve(request, () -> { throw new AssertionError("Ordinary routing must not execute"); }, _ -> { throw new AssertionError("Backend selection must not execute"); }));
        verifyNoInteractions(store, selector, backendManager, httpClient);
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void noncanonicalContinuationCannotFallThroughToOrdinaryRouting(String method)
    {
        for (String path : List.of(
                "/v1/statement;parameter/executing/" + QUERY + "/capability/1",
                "/v1/%73tatement/executing/" + QUERY + "/capability/1",
                "/v1/./statement/executing/" + QUERY + "/capability/1",
                "/v1/other/../statement/executing/" + QUERY + "/capability/1",
                "/v1//statement/executing/" + QUERY + "/capability/1",
                "/v1\\statement/executing/" + QUERY + "/capability/1",
                "/v1/query;parameter/" + QUERY,
                "/v1/%71uery/" + QUERY,
                "/v1/./query/" + QUERY,
                "/v1//query/" + QUERY)) {
            HttpServletRequest request = request(method, path, Map.of());
            expectStatus(400, () -> service.resolve(
                    request,
                    () -> { throw new AssertionError("Noncanonical continuation reached ordinary routing: " + path); },
                    _ -> { throw new AssertionError("Noncanonical continuation reached backend selection: " + path); }));
        }
        verifyNoInteractions(store, selector, backendManager, httpClient);
    }

    @ParameterizedTest
    @ValueSource(strings = {"X-Trino-Query-Data-Encoding", "x-trino-query-data-encoding", "X-TRINO-QUERY-DATA-ENCODING"})
    void inlineNegotiationRemovesAdvertisementOnlyFromForwardedRequest(String header)
    {
        String basic = "Basic " + Base64.getEncoder().encodeToString("alice:synthetic-password".getBytes(UTF_8));
        HttpServletRequest request = request("POST", "/v1/statement", Map.of("Authorization", List.of(basic), header, List.of("json+zstd,json+lz4,json")));
        Admission admission = new Admission(UUID.randomUUID(), BACKEND, "owner", null, null);
        when(selector.findRoutingDestination(request)).thenReturn(new RoutingSelectorResponse("group"));
        when(store.getRoute("group")).thenReturn(Optional.of("blue"));
        when(store.admitNew(eq("blue"), anyString(), eq("group"))).thenReturn(admission);
        StringResponse process = mock(StringResponse.class);
        when(process.getStatusCode()).thenReturn(200);
        when(process.getBody()).thenReturn("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node\",\"coordinatorId\":\"abcde\"}");
        when(httpClient.execute(any(), any())).thenReturn(process);
        RoutingTargetResponse resolved = service.resolve(request, () -> { throw new AssertionError("Ordinary routing must not execute"); }, _ -> { throw new AssertionError("Override must bypass candidate selection"); });
        HttpServletRequest forwarded = resolved.modifiedRequest();
        assertThat(forwarded.getHeader(header)).isNull();
        assertThat(Collections.list(forwarded.getHeaders(header))).isEmpty();
        assertThat(Collections.list(forwarded.getHeaderNames())).noneMatch(name -> name.equalsIgnoreCase(header));
        assertThat(request.getHeader(header)).isEqualTo("json+zstd,json+lz4,json");
        assertThat(forwarded.getHeader("Authorization")).isEqualTo(basic);
        assertThat(admission(forwarded)).isEqualTo(admission);
        service.recordResponse(forwarded, response(200, RESULTS));
        verify(store).recordResponse(admission.id(), new ResponseObservation(QUERY, null, false, true, 120));
    }

    @Test
    void disabledFeaturePreservesOrdinaryRouting()
    {
        configuration.getTransactionAwareness().setEnabled(false);
        HttpServletRequest request = request("POST", "/v1/statement/", Map.of());
        RoutingTargetResponse ordinary = new RoutingTargetResponse(new RoutingDestination("group", BACKEND.url(), URI.create(BACKEND.url()), BACKEND.externalUrl()), request);
        assertThat(service.resolve(request, () -> ordinary, _ -> { throw new AssertionError("Unused backend selector"); })).isSameAs(ordinary);
        verifyNoInteractions(store, selector, backendManager, httpClient);
    }

    @Test
    void canonicalPathDoesNotRejectEncodedQueryParameters()
    {
        HttpServletRequest request = request("GET", "/v1/info", Map.of());
        when(request.getQueryString()).thenReturn("example=%2Fpath%3Bvalue&dot=../segment");
        RoutingTargetResponse ordinary = new RoutingTargetResponse(new RoutingDestination("group", BACKEND.url(), URI.create(BACKEND.url()), BACKEND.externalUrl()), request);
        assertThat(service.resolve(request, () -> ordinary, _ -> { throw new AssertionError("Unused backend selector"); })).isSameAs(ordinary);
        verifyNoInteractions(store, selector, backendManager, httpClient);
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void queryIdWithoutCapabilityRequiresCredentials(String method)
    {
        configureKnownQuery();
        HttpServletRequest request = request(method, "/v1/query/" + QUERY, Map.of());
        expectStatus(401, () -> service.resolve(
                request,
                () -> { throw new AssertionError("Known query must not use ordinary routing"); },
                _ -> { throw new AssertionError("Known query must not select a new backend"); }));
        verify(store, never()).admitQuery(anyString(), any(), any(), any());
        verifyNoInteractions(httpClient);
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void resultCapabilityStillSupportsHeaderlessContinuation(String method)
    {
        Admission admission = configureKnownQuery();
        HttpServletRequest request = request(method, CONTINUATION, Map.of());
        RoutingTargetResponse response = service.resolve(
                request,
                () -> { throw new AssertionError("Known query must not use ordinary routing"); },
                _ -> { throw new AssertionError("Known query must not select a new backend"); });
        assertThat(admission(response.modifiedRequest())).isEqualTo(admission);
        assertThat(response.routingDestination().clusterHost()).isEqualTo(BACKEND.url());
        verify(store).admitQuery(QUERY, Optional.of("owner"), Optional.empty(), Optional.of(TransactionAwarenessService.capabilityHash(CONTINUATION)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void unadvertisedCapabilityCannotReachBackend(String method)
    {
        configureKnownQuery();
        String forged = CONTINUATION.replace("capability", "forged");
        when(store.admitQuery(eq(QUERY), any(), any(), eq(Optional.of(TransactionAwarenessService.capabilityHash(forged)))))
                .thenThrow(new StoreException(ErrorCode.NOT_FOUND, "Unknown capability"));
        HttpServletRequest request = request(method, forged, Map.of());
        expectStatus(404, () -> service.resolve(
                request,
                () -> { throw new AssertionError("Capability rejection must not reroute"); },
                _ -> { throw new AssertionError("Capability rejection must not select a backend"); }));
        assertThat(admission(request)).isNull();
        verify(store, never()).admitQuery(anyString(), any(), any());
        verify(store, never()).admitQuery(anyString(), any(), any(), eq(Optional.empty()));
        verifyNoInteractions(httpClient, selector, backendManager);
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void validCapabilityPreservesOriginalPathAndQueryParameters(String method)
    {
        configureKnownQuery();
        HttpServletRequest request = request(method, CONTINUATION, Map.of());
        when(request.getQueryString()).thenReturn("maxWait=1s&example=%2Fencoded");
        RoutingTargetResponse resolved = service.resolve(
                request,
                () -> { throw new AssertionError("Known query must not reroute"); },
                _ -> { throw new AssertionError("Known query must not select a backend"); });
        assertThat(resolved.modifiedRequest().getRequestURI()).isEqualTo(CONTINUATION);
        assertThat(resolved.modifiedRequest().getQueryString()).isEqualTo(request.getQueryString());
        assertThat(resolved.routingDestination().clusterUri().getRawPath()).isEqualTo(CONTINUATION);
        assertThat(resolved.routingDestination().clusterUri().getRawQuery()).isEqualTo(request.getQueryString());
        verify(store).admitQuery(QUERY, Optional.of("owner"), Optional.empty(), Optional.of(TransactionAwarenessService.capabilityHash(CONTINUATION)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "DELETE"})
    void authenticatedMetadataUsesOwnerWithoutResultCapability(String method)
    {
        configureKnownQuery();
        String basic = "Basic " + Base64.getEncoder().encodeToString("alice:synthetic-password".getBytes(UTF_8));
        HttpServletRequest request = request(method, "/v1/query/" + QUERY, Map.of("Authorization", List.of(basic)));
        String owner = new TransactionIdentity(configuration.getTransactionAwareness().getIdentityKey()).owner(request);
        when(store.getQuery(QUERY)).thenReturn(Optional.of(new QueryBinding(QUERY, owner, BACKEND, null, false)));
        service.resolve(request, () -> { throw new AssertionError("Metadata must not reroute"); }, _ -> { throw new AssertionError("Metadata must not select a backend"); });
        verify(store).admitQuery(QUERY, Optional.of(owner), Optional.empty(), Optional.empty());
    }

    @Test
    void capabilityHashMatchesCanonicalRawPathSha256()
    {
        assertThat(TransactionAwarenessService.capabilityHash(CONTINUATION)).isEqualTo("528710e9ad15ed24ba4252d2c744d8fbe2c3818700e5f0d791237d355154bcb6");
    }

    @Test
    void advertisedNextAndPartialCancelCapabilitiesShareAtomicObservation()
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, null);
        String next = CONTINUATION.replace("/1", "/2");
        String cancel = "/v1/statement/executing/partialCancel/" + QUERY + "/1/capability/1";
        String body = RESULTS.substring(0, RESULTS.length() - 1) + ",\"nextUri\":\"https://gateway.example.test" + next + "?maxWait=1s\",\"partialCancelUri\":\"http://blue.example.test" + cancel + "\"}";
        ProxyResponse response = response(200, body);
        assertThat(service.recordResponse(request, response)).isSameAs(response);
        assertThat(response.body()).isEqualTo(body);
        verify(store).recordResponse(admission(request).id(), new ResponseObservation(
                QUERY,
                null,
                false,
                false,
                120,
                List.of(TransactionAwarenessService.capabilityHash(next), TransactionAwarenessService.capabilityHash(cancel))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"nextUri", "partialCancelUri"})
    void invalidAdvertisedCapabilityCannotSettleAnyLifecycle(String field)
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, TRANSACTION);
        for (String uri : List.of(
                "http://blue.example.test" + CONTINUATION.replace(QUERY, "20260909_120000_00002_abcde"),
                "http://blue.example.test" + CONTINUATION.replace("statement", "%73tatement"),
                "http://blue.example.test" + CONTINUATION.replace("/executing/", "/./executing/"),
                "http://blue.example.test" + CONTINUATION + "#fragment",
                "http://user@blue.example.test" + CONTINUATION,
                "ftp://blue.example.test" + CONTINUATION,
                CONTINUATION,
                "http://[invalid")) {
            String body = RESULTS.substring(0, RESULTS.length() - 1) + ",\"" + field + "\":\"" + uri + "\"}";
            expectStatus(502, () -> service.recordResponse(request, response(200, body, "X-Trino-Clear-Transaction-Id", "true")));
        }
        verifyNoInteractions(store);
    }

    @Test
    void malformedPartialCapabilityCannotPartiallyRecordValidNextCapability()
    {
        HttpServletRequest request = admitted("GET", CONTINUATION, QUERY, null);
        String body = RESULTS.substring(0, RESULTS.length() - 1) + ",\"nextUri\":\"http://blue.example.test" + CONTINUATION + "\",\"partialCancelUri\":42}";
        expectStatus(502, () -> service.recordResponse(request, response(200, body)));
        verifyNoInteractions(store);
    }

    private Admission configureKnownQuery()
    {
        Admission admission = new Admission(UUID.randomUUID(), BACKEND, "owner", null, QUERY);
        when(store.getQuery(QUERY)).thenReturn(Optional.of(new QueryBinding(QUERY, "owner", BACKEND, null, false)));
        when(store.admitQuery(eq(QUERY), any(), any(), any())).thenReturn(admission);
        StringResponse process = mock(StringResponse.class);
        when(process.getStatusCode()).thenReturn(200);
        when(process.getBody()).thenReturn("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node\",\"coordinatorId\":\"abcde\"}");
        when(httpClient.execute(any(), any())).thenReturn(process);
        return admission;
    }

    private static HttpServletRequest admitted(String method, String path, String queryId, String transactionId)
    {
        HttpServletRequest request = request(method, path, Map.of());
        request.setAttribute(TransactionAwarenessService.class.getName() + ".admission", new Admission(UUID.randomUUID(), BACKEND, "owner", transactionId, queryId));
        return request;
    }

    private static Admission admission(HttpServletRequest request)
    {
        return (Admission) request.getAttribute(TransactionAwarenessService.class.getName() + ".admission");
    }

    private static HttpServletRequest request(String method, String path, Map<String, List<String>> suppliedHeaders)
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.putAll(suppliedHeaders);
        Map<String, Object> attributes = new HashMap<>();
        when(request.getMethod()).thenReturn(method);
        when(request.getRequestURI()).thenReturn(path);
        when(request.getHeaders(anyString())).thenAnswer(call -> Collections.enumeration(headers.getOrDefault(call.getArgument(0), List.of())));
        when(request.getHeader(anyString())).thenAnswer(call -> headers.getOrDefault(call.getArgument(0), List.of()).stream().findFirst().orElse(null));
        when(request.getHeaderNames()).thenAnswer(_ -> Collections.enumeration(headers.keySet()));
        when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
        doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1))).when(request).setAttribute(anyString(), any());
        return request;
    }

    private static ProxyResponse response(int status, String body, String... headers)
    {
        ImmutableListMultimap.Builder<HeaderName, String> values = ImmutableListMultimap.builder();
        for (int index = 0; index < headers.length; index += 2) {
            values.put(HeaderName.of(headers[index]), headers[index + 1]);
        }
        return new ProxyResponse(status, values.build(), body);
    }

    private static void expectStatus(int status, Runnable action)
    {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(status));
    }
}
