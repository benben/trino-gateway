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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.gateway.ha.config.GatewayCookieConfigurationPropertiesProvider;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.config.ProxyResponseConfiguration;
import io.trino.gateway.ha.handler.schema.RoutingDestination;
import io.trino.gateway.ha.router.GatewayCookie;
import io.trino.gateway.ha.router.OAuth2GatewayCookie;
import io.trino.gateway.ha.router.QueryHistoryManager;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.router.TrinoRequestUser;
import io.trino.gateway.ha.transaction.TransactionAwarenessService;
import io.trino.gateway.ha.transaction.TransactionAwarenessService.RequestContext;
import io.trino.gateway.proxyserver.ProxyResponseHandler.ProxyResponse;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HeaderNames.VIA;
import static io.airlift.http.client.HeaderNames.X_FORWARDED_FOR;
import static io.airlift.http.client.HeaderNames.X_FORWARDED_HOST;
import static io.airlift.http.client.HeaderNames.X_FORWARDED_PORT;
import static io.airlift.http.client.HeaderNames.X_FORWARDED_PROTO;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.gateway.ha.handler.HttpUtils.TRINO_REQUEST_USER;
import static io.trino.gateway.ha.handler.ProxyUtils.SOURCE_HEADER;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.BAD_GATEWAY;
import static jakarta.ws.rs.core.Response.Status.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class ProxyRequestHandler
{
    private static final Logger log = Logger.get(ProxyRequestHandler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> PRESERVED_HEADERS_TO_SKIP = List.of(
            "Accept-Encoding",
            "Host");

    private final Duration asyncTimeout;
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("proxy-%s"));
    private final HttpClient httpClient;
    private final RoutingManager routingManager;
    private final QueryHistoryManager queryHistoryManager;
    private final boolean cookiesEnabled;
    private final boolean forwardedHeadersEnabled;
    private final List<String> statementPaths;
    private final boolean includeClusterInfoInResponse;
    private final ProxyResponseConfiguration proxyResponseConfiguration;
    private TransactionAwarenessService transactionAwareness;

    @Inject
    public void setTransactionAwareness(TransactionAwarenessService transactionAwareness)
    {
        this.transactionAwareness = transactionAwareness;
    }

    @Inject
    public ProxyRequestHandler(
            @ForProxy HttpClient httpClient,
            RoutingManager routingManager,
            QueryHistoryManager queryHistoryManager,
            HaGatewayConfiguration haGatewayConfiguration)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.routingManager = requireNonNull(routingManager, "routingManager is null");
        this.queryHistoryManager = requireNonNull(queryHistoryManager, "queryHistoryManager is null");
        cookiesEnabled = GatewayCookieConfigurationPropertiesProvider.getInstance().isEnabled();
        asyncTimeout = haGatewayConfiguration.getRouting().getAsyncTimeout();
        forwardedHeadersEnabled = haGatewayConfiguration.getRouting().isForwardedHeadersEnabled();
        statementPaths = haGatewayConfiguration.getStatementPaths();
        this.includeClusterInfoInResponse = haGatewayConfiguration.isIncludeClusterHostInResponse();
        proxyResponseConfiguration = haGatewayConfiguration.getProxyResponseConfiguration();
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    public void deleteRequest(
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            RoutingDestination routingDestination)
    {
        Request.Builder request = prepareDelete();
        performRequest(routingDestination, servletRequest, asyncResponse, request);
    }

    public void getRequest(
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            RoutingDestination routingDestination)
    {
        Request.Builder request = prepareGet();
        performRequest(routingDestination, servletRequest, asyncResponse, request);
    }

    public void postRequest(
            String statement,
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            RoutingDestination routingDestination)
    {
        Request.Builder request = preparePost()
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8));
        performRequest(routingDestination, servletRequest, asyncResponse, request);
    }

    public void putRequest(
            String statement,
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            RoutingDestination routingDestination)
    {
        Request.Builder request = preparePut()
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8));
        performRequest(routingDestination, servletRequest, asyncResponse, request);
    }

    public void headRequest(
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            RoutingDestination routingDestination)
    {
        Request.Builder request = prepareHead();
        performRequest(routingDestination, servletRequest, asyncResponse, request);
    }

    private void performRequest(
            RoutingDestination routingDestination,
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            Request.Builder requestBuilder)
    {
        RequestContext context = transactionAwareness == null ? null : transactionAwareness.captureRequestContext(servletRequest);
        try {
            URI remoteUri = routingDestination.clusterUri();
            requestBuilder.setUri(remoteUri);

            setupRequestHeaders(servletRequest, requestBuilder);

            ImmutableList.Builder<NewCookie> cookieBuilder = ImmutableList.builder();
            cookieBuilder.addAll(getOAuth2GatewayCookie(remoteUri, servletRequest));

            Request request = requestBuilder
                    .setFollowRedirects(false)
                    .build();

            if (context != null) {
                performBoundedRequest(routingDestination, context, asyncResponse, requestBuilder, cookieBuilder);
                return;
            }

            FluentFuture<ProxyResponse> future = executeHttp(request);

            if (transactionAwareness != null && transactionAwareness.isEnabled()) {
                future = future.transform(response -> transactionAwareness.recordResponse(servletRequest, response), executor)
                        .catching(Exception.class, exception -> {
                            transactionAwareness.requestFailed(servletRequest);
                            if (exception instanceof WebApplicationException webException) {
                                throw webException;
                            }
                            throw new WebApplicationException(Response.status(BAD_GATEWAY).type(TEXT_PLAIN_TYPE)
                                    .entity("Backend request outcome is uncertain; the request was not reassigned").build());
                        }, directExecutor());
            }

            if (statementPaths.stream().anyMatch(request.getUri().getPath()::startsWith) && request.getMethod().equals(HttpMethod.POST)) {
                Optional<String> username = ((TrinoRequestUser) servletRequest.getAttribute(TRINO_REQUEST_USER)).getUser();
                future = future.transform(response -> recordBackendForQueryId(request, response, username, routingDestination), executor);
                if (includeClusterInfoInResponse) {
                    cookieBuilder.add(new NewCookie.Builder("trinoClusterHost").value(remoteUri.getHost()).build());
                }
            }

            setupAsyncResponse(
                    asyncResponse,
                    future.transform(response -> buildResponse(response, cookieBuilder.build()), executor)
                            .catching(ProxyException.class, e -> handleProxyException(request, e), directExecutor()));
        }
        catch (RuntimeException failure) {
            if (context != null && !context.isCompletionManaged()) {
                if (context.wasDispatched()) {
                    try {
                        transactionAwareness.completionPhase(() -> {
                            transactionAwareness.requestFailed(context);
                            return null;
                        });
                    }
                    finally {
                        context.close();
                    }
                }
                else {
                    transactionAwareness.requestRejectedBeforeDispatch(context);
                }
            }
            throw failure;
        }
    }

    private void performBoundedRequest(
            RoutingDestination destination,
            RequestContext context,
            AsyncResponse asyncResponse,
            Request.Builder requestBuilder,
            ImmutableList.Builder<NewCookie> cookieBuilder)
    {
        Duration remaining = context.remainingTime();
        Request request = requestBuilder.setRequestTimeout(remaining).setIdleTimeout(remaining).build();
        context.beforeDispatch();
        FluentFuture<ProxyResponse> backend = executeHttp(request);
        SettableFuture<Response> completed = SettableFuture.create();
        AtomicBoolean completionStarted = new AtomicBoolean();
        addCallback(backend, new FutureCallback<>()
        {
            @Override
            public void onSuccess(ProxyResponse response)
            {
                finish(response, null);
            }

            @Override
            public void onFailure(Throwable failure)
            {
                finish(null, failure);
            }

            private void finish(ProxyResponse response, Throwable failure)
            {
                if (!completionStarted.compareAndSet(false, true)) {
                    return;
                }
                try {
                    if (failure != null) {
                        throw new ProxyException("Backend request outcome is uncertain", failure);
                    }
                    Response result = transactionAwareness.completionPhase(() -> {
                        ProxyResponse recorded = transactionAwareness.recordResponse(context, response);
                        if (request.getMethod().equals(HttpMethod.POST)) {
                            recorded = recordBackendForQueryId(request, recorded, context.username(), destination);
                            if (includeClusterInfoInResponse) {
                                cookieBuilder.add(new NewCookie.Builder("trinoClusterHost").value(request.getUri().getHost()).build());
                            }
                        }
                        return buildResponse(recorded, cookieBuilder.build());
                    });
                    completed.set(result);
                }
                catch (Throwable outcomeFailure) {
                    try {
                        transactionAwareness.completionPhase(() -> {
                            transactionAwareness.requestFailed(context);
                            return null;
                        });
                    }
                    catch (RuntimeException recordingFailure) {
                        outcomeFailure.addSuppressed(recordingFailure);
                    }
                    completed.setException(outcomeFailure instanceof WebApplicationException ? outcomeFailure :
                            new WebApplicationException(Response.status(BAD_GATEWAY).type(TEXT_PLAIN_TYPE)
                                                        .entity("Backend request outcome is uncertain; the request was not reassigned").build()));
                }
                finally {
                    context.close();
                }
            }
        }, transactionAwareness.completionExecutor());
        context.manageCompletion();
        Duration clientTimeout = new Duration(Math.min(asyncTimeout.toMillis(), Math.max(1, remaining.toMillis())), java.util.concurrent.TimeUnit.MILLISECONDS);
        bindAsyncResponse(asyncResponse, nonCancellationPropagating(completed), directExecutor())
                .withTimeout(clientTimeout, () -> Response.status(BAD_GATEWAY).type(TEXT_PLAIN_TYPE)
                        .entity("Transaction-aware response deadline expired; outcome processing remains bounded and retained").build());
    }

    private ImmutableList<NewCookie> getOAuth2GatewayCookie(URI remoteUri, HttpServletRequest servletRequest)
    {
        if (cookiesEnabled) {
            if (remoteUri.getPath().startsWith(OAuth2GatewayCookie.OAUTH2_PATH)
                    && !(servletRequest.getCookies() != null
                    && Arrays.stream(servletRequest.getCookies()).anyMatch(c -> c.getName().equals(OAuth2GatewayCookie.NAME)))) {
                GatewayCookie oauth2Cookie = new OAuth2GatewayCookie(getRemoteTarget(remoteUri));
                return ImmutableList.of(oauth2Cookie.toNewCookie());
            }
            else if (servletRequest.getCookies() != null) {
                return Arrays.stream(servletRequest.getCookies())
                        .filter(c -> c.getName().startsWith(GatewayCookie.PREFIX))
                        .map(GatewayCookie::fromCookie)
                        .filter(c -> !c.isValid() || c.matchesDeletePath(remoteUri.getPath()))
                        .map(GatewayCookie::toNewCookie)
                        .map(newCookie -> new NewCookie.Builder(newCookie).value("delete").maxAge(0).build())
                        .collect(toImmutableList());
            }
        }
        return ImmutableList.of();
    }

    private static String getRemoteTarget(URI remoteUri)
    {
        return "%s://%s".formatted(remoteUri.getScheme(), remoteUri.getAuthority());
    }

    private Response buildResponse(ProxyResponse response, ImmutableList<NewCookie> cookie)
    {
        Response.ResponseBuilder builder = Response.status(response.statusCode()).entity(response.body());
        response.headers().forEach((headerName, value) -> builder.header(headerName.toString(), value));
        cookie.forEach(builder::cookie);
        return builder.build();
    }

    private void setupAsyncResponse(AsyncResponse asyncResponse, ListenableFuture<Response> future)
    {
        bindAsyncResponse(asyncResponse, future, executor)
                .withTimeout(asyncTimeout, () -> Response
                        .status(BAD_GATEWAY)
                        .type(TEXT_PLAIN_TYPE)
                        .entity("Request to remote Trino server timed out after" + asyncTimeout)
                        .build());
    }

    private FluentFuture<ProxyResponse> executeHttp(Request request)
    {
        return FluentFuture.from(httpClient.executeAsync(request, new ProxyResponseHandler(proxyResponseConfiguration, transactionAwareness != null && transactionAwareness.isEnabled())));
    }

    private static Response handleProxyException(Request request, ProxyException e)
    {
        log.warn(e, "Proxy request failed: %s %s", request.getMethod(), request.getUri());
        throw badRequest(e.getMessage());
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(
                Response.status(Response.Status.BAD_GATEWAY)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private ProxyResponse recordBackendForQueryId(
            Request request,
            ProxyResponse response,
            Optional<String> username,
            RoutingDestination routingDestination)
    {
        log.debug("For Request [%s] got Response [%s]", request.getUri(), response.body());

        QueryHistoryManager.QueryDetail queryDetail = getQueryDetailsFromRequest(request, username);

        log.debug("Extracting proxy destination : [%s] for request : [%s]", queryDetail.getBackendUrl(), request.getUri());

        if (response.statusCode() == OK.getStatusCode()) {
            try {
                HashMap<String, String> results = OBJECT_MAPPER.readValue(response.body(), HashMap.class);
                queryDetail.setQueryId(results.get("id"));
                routingManager.setBackendForQueryId(queryDetail.getQueryId(), queryDetail.getBackendUrl());
                routingManager.setRoutingGroupForQueryId(queryDetail.getQueryId(), routingDestination.routingGroup());
                routingManager.setExternalUrlForQueryId(queryDetail.getQueryId(), routingDestination.externalUrl());
                log.debug("QueryId [%s] mapped with proxy [%s]", queryDetail.getQueryId(), queryDetail.getBackendUrl());
            }
            catch (IOException e) {
                log.error("Failed to get QueryId from response [%s] , Status code [%s]", response.body(), response.statusCode());
            }
        }
        else {
            log.error("Non OK HTTP Status code with response [%s] , Status code [%s], user: [%s]", response.body(), response.statusCode(), username.orElse(null));
        }
        queryDetail.setRoutingGroup(routingDestination.routingGroup());
        queryDetail.setExternalUrl(routingDestination.externalUrl());
        queryHistoryManager.submitQueryDetail(queryDetail);
        return response;
    }

    public static QueryHistoryManager.QueryDetail getQueryDetailsFromRequest(Request request, Optional<String> username)
    {
        QueryHistoryManager.QueryDetail queryDetail = new QueryHistoryManager.QueryDetail();
        queryDetail.setBackendUrl(getRemoteTarget(request.getUri()));
        queryDetail.setCaptureTime(System.currentTimeMillis());
        username.ifPresent(queryDetail::setUser);
        queryDetail.setSource(request.getHeader(SOURCE_HEADER));

        String queryText = new String(((StaticBodyGenerator) request.getBodyGenerator()).getBody(), UTF_8);
        queryDetail.setQueryText(queryText);
        return queryDetail;
    }

    private void setupRequestHeaders(HttpServletRequest servletRequest, Request.Builder requestBuilder)
    {
        for (String name : list(servletRequest.getHeaderNames())) {
            if (shouldForwardHeader(name)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(HeaderName.of(name), value);
                }
            }
        }

        requestBuilder.addHeader(VIA, "%s TrinoGateway".formatted(servletRequest.getProtocol()));

        if (forwardedHeadersEnabled) {
            addForwardedHeaders(servletRequest, requestBuilder);
        }
    }

    private static boolean isForwardedHeader(String name)
    {
        return name.regionMatches(true, 0, "X-Forwarded-", 0, 12)
                || name.equalsIgnoreCase("Forwarded");
    }

    // TODO: decide what else should and shouldn't be forwarded
    private boolean shouldForwardHeader(String name)
    {
        for (String headerToSkip : PRESERVED_HEADERS_TO_SKIP) {
            if (name.equalsIgnoreCase(headerToSkip)) {
                return false;
            }
        }
        if (isForwardedHeader(name) && !forwardedHeadersEnabled) {
            return false;
        }
        return true;
    }

    private static void addForwardedHeaders(HttpServletRequest servletRequest, Request.Builder requestBuilder)
    {
        requestBuilder.addHeader(X_FORWARDED_FOR, servletRequest.getRemoteAddr());
        requestBuilder.addHeader(X_FORWARDED_PROTO, servletRequest.getScheme());
        requestBuilder.addHeader(X_FORWARDED_PORT, String.valueOf(servletRequest.getServerPort()));
        String serverName = servletRequest.getServerName();
        if (serverName != null) {
            requestBuilder.addHeader(X_FORWARDED_HOST, serverName);
        }
    }
}
