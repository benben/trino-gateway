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
package io.trino.gateway.proxyserver;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.trino.gateway.ha.config.DataStoreConfiguration;
import io.trino.gateway.ha.config.GatewayCookieConfigurationPropertiesProvider;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.handler.schema.RoutingTargetResponse;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.QueryHistoryManager;
import io.trino.gateway.ha.router.RoutingGroupSelector;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.transaction.TransactionAwarenessService;
import io.trino.gateway.ha.transaction.TransactionStore;
import io.trino.gateway.ha.transaction.TransactionStore.Admission;
import io.trino.gateway.ha.transaction.TransactionStore.BackendRef;
import io.trino.gateway.ha.transaction.TransactionStore.QueryBinding;
import io.trino.gateway.proxyserver.ProxyResponseHandler.ProxyResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.TimeoutHandler;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestTransactionBoundedProxy
{
    private static final String QUERY = "20260910_120000_00001_abcde";
    private static final String PATH = "/v1/statement/executing/" + QUERY + "/capability/1";
    private static final BackendRef BACKEND = new BackendRef("blue", UUID.randomUUID(), "http://blue.example.test", "http://blue.example.test", "group", "node", "abcde");
    private MockedConstruction<TransactionStore> construction;
    private TransactionStore store;
    private TransactionAwarenessService service;
    private ProxyRequestHandler handler;
    private HttpClient proxy;
    private SettableFuture<ProxyResponse> raw;

    @BeforeEach
    void setup()
    {
        construction = mockConstruction(TransactionStore.class);
        HaGatewayConfiguration config = new HaGatewayConfiguration();
        DataStoreConfiguration database = new DataStoreConfiguration();
        database.setJdbcUrl("jdbc:postgresql://localhost/unused_unit_test_database");
        config.setDataStore(database);
        config.getTransactionAwareness().setEnabled(true);
        config.getTransactionAwareness().setIdentityKey("synthetic-identity-key-for-adapter-tests");
        config.getTransactionAwareness().setAdminToken("synthetic-admin-key-for-adapter-tests");
        config.getTransactionAwareness().setMaxInFlightRequests(1);
        config.getTransactionAwareness().setCompletionThreads(1);
        GatewayCookieConfigurationPropertiesProvider.getInstance().initialize(config.getGatewayCookieConfiguration());
        HttpClient monitor = mock(HttpClient.class);
        StringResponse info = mock(StringResponse.class);
        when(info.getStatusCode()).thenReturn(200);
        when(info.getBody()).thenReturn("{\"coordinator\":true,\"starting\":false,\"nodeId\":\"node\",\"coordinatorId\":\"abcde\"}");
        when(monitor.execute(any(), any())).thenReturn(info);
        service = new TransactionAwarenessService(config, mock(Jdbi.class), mock(GatewayBackendManager.class), monitor, mock(RoutingGroupSelector.class));
        store = construction.constructed().getFirst();
        when(store.getQuery(QUERY)).thenReturn(Optional.of(new QueryBinding(QUERY, "owner", BACKEND, null, false)));
        when(store.admitQuery(anyString(), any(), any(), any())).thenAnswer(_ -> new Admission(UUID.randomUUID(), BACKEND, "owner", null, QUERY));
        proxy = mock(HttpClient.class);
        raw = SettableFuture.create();
        HttpResponseFuture<?> future = new TestingResponseFuture(raw);
        doReturn(future).when(proxy).executeAsync(any(), any());
        handler = new ProxyRequestHandler(proxy, mock(RoutingManager.class), mock(QueryHistoryManager.class), config);
        handler.setTransactionAwareness(service);
    }

    @AfterEach
    void shutdown()
    {
        handler.shutdown();
        service.shutdown();
        construction.close();
    }

    @Test
    void clientTimeoutDoesNotCancelBackendOrReleaseCompletionReservation()
            throws Exception
    {
        RoutingTargetResponse target = resolve();
        AsyncResponse client = mock(AsyncResponse.class);
        handler.getRequest(target.modifiedRequest(), client, target.routingDestination());
        ArgumentCaptor<TimeoutHandler> timeout = ArgumentCaptor.forClass(TimeoutHandler.class);
        verify(client).setTimeoutHandler(timeout.capture());
        timeout.getValue().handleTimeout(client);
        assertThat(raw.isCancelled()).isFalse();
        assertOverloaded();

        CountDownLatch processing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(_ -> {
            processing.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(store).recordResponse(any(), any());
        raw.set(results());
        try {
            assertThat(processing.await(5, TimeUnit.SECONDS)).isTrue();
            assertOverloaded();
            verify(store, never()).markUncertain(any());
        }
        finally {
            release.countDown();
        }
        awaitAvailable();
        verify(store).recordResponse(any(), any());
    }

    @Test
    void synchronousDispatchFailureRetainsUncertaintyAndReleasesCapacity()
    {
        RoutingTargetResponse target = resolve();
        when(proxy.executeAsync(any(), any())).thenThrow(new IllegalStateException("synthetic queue rejection"));
        assertThatThrownBy(() -> handler.getRequest(target.modifiedRequest(), mock(AsyncResponse.class), target.routingDestination()))
                .isInstanceOf(IllegalStateException.class);
        verify(store).markUncertain(any());
        service.requestRejectedBeforeDispatch(resolve().modifiedRequest());
    }

    @Test
    void shutdownRejectsNewWorkButPreservesReservedCompletion()
            throws Exception
    {
        RoutingTargetResponse target = resolve();
        handler.getRequest(target.modifiedRequest(), mock(AsyncResponse.class), target.routingDestination());
        service.shutdown();
        assertOverloaded();
        CountDownLatch finished = new CountDownLatch(1);
        doAnswer(_ -> {
            finished.countDown();
            return null;
        }).when(store).recordResponse(any(), any());
        raw.set(results());
        assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void clientResponseBindingFailureDoesNotReleaseRunningCompletion()
            throws Exception
    {
        RoutingTargetResponse target = resolve();
        AsyncResponse client = mock(AsyncResponse.class);
        doAnswer(_ -> { throw new IllegalStateException("synthetic client binding failure"); }).when(client).setTimeoutHandler(any());
        assertThatThrownBy(() -> handler.getRequest(target.modifiedRequest(), client, target.routingDestination()))
                .isInstanceOf(IllegalStateException.class);
        assertOverloaded();
        raw.set(results());
        awaitAvailable();
        verify(store, never()).markUncertain(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAD", "DELETE"})
    void bodylessProtocolResponsesReleaseTheirCapacity(String method)
            throws Exception
    {
        RoutingTargetResponse target = resolve(method);
        if (method.equals("HEAD")) {
            handler.headRequest(target.modifiedRequest(), mock(AsyncResponse.class), target.routingDestination());
        }
        else {
            handler.deleteRequest(target.modifiedRequest(), mock(AsyncResponse.class), target.routingDestination());
        }
        raw.set(new ProxyResponse(method.equals("HEAD") ? 200 : 204, ImmutableListMultimap.of(), ""));
        awaitAvailable();
        verify(store, never()).markUncertain(any());
    }

    private RoutingTargetResponse resolve()
    {
        return resolve("GET");
    }

    private RoutingTargetResponse resolve(String method)
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        Map<String, Object> attributes = new HashMap<>();
        when(request.getMethod()).thenReturn(method);
        when(request.getRequestURI()).thenReturn(PATH);
        when(request.getHeaders(anyString())).thenAnswer(_ -> Collections.emptyEnumeration());
        when(request.getHeaderNames()).thenAnswer(_ -> Collections.emptyEnumeration());
        when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
        doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1))).when(request).setAttribute(anyString(), any());
        return service.resolve(request, () -> { throw new AssertionError("Unexpected ordinary routing"); }, _ -> { throw new AssertionError("Unexpected backend selection"); });
    }

    private void assertOverloaded()
    {
        assertThatThrownBy(this::resolve).isInstanceOfSatisfying(
                WebApplicationException.class,
                failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(503));
    }

    private void awaitAvailable()
            throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            try {
                service.requestRejectedBeforeDispatch(resolve().modifiedRequest());
                return;
            }
            catch (WebApplicationException failure) {
                assertThat(failure.getResponse().getStatus()).isEqualTo(503);
                assertThat(System.nanoTime()).isLessThan(deadline);
                TimeUnit.MILLISECONDS.sleep(5);
            }
        }
    }

    private static ProxyResponse results()
    {
        return new ProxyResponse(200, ImmutableListMultimap.of(), "{\"id\":\"" + QUERY + "\",\"stats\":{\"state\":\"FINISHED\"}}");
    }

    private static final class TestingResponseFuture
            extends ForwardingListenableFuture.SimpleForwardingListenableFuture<ProxyResponse>
            implements HttpResponseFuture<ProxyResponse>
    {
        private TestingResponseFuture(SettableFuture<ProxyResponse> delegate)
        {
            super(delegate);
        }

        @Override
        public String getState()
        {
            return isDone() ? "DONE" : "WAITING";
        }
    }
}
