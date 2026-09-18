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
package io.trino.gateway.ha.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.trino.gateway.ha.transaction.PoolLifecycleService;
import io.trino.gateway.ha.transaction.PoolStore;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

import java.util.List;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Versioned pooled member lifecycle protocol (protocol version 1). Absent unless
 * {@code transactionAwareness.pool.enabled} is set; the old rollout and cutover APIs are unaffected.
 * Authentication reuses the existing administration token — no new credential is introduced.
 */
@Path("/gateway/v1/pools")
@Produces(APPLICATION_JSON)
@RolesAllowed("API")
public class PoolResource
{
    private final PoolLifecycleService service;

    @Inject
    public PoolResource(PoolLifecycleService service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @GET
    @Path("/{poolId}")
    public PoolStore.PoolState pool(@PathParam("poolId") String poolId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.poolState(poolId);
    }

    @PUT
    @Path("/{poolId}")
    @Consumes(APPLICATION_JSON)
    public PoolStore.PoolState configure(@PathParam("poolId") String poolId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.configurePool(poolId, body);
    }

    @GET
    @Path("/{poolId}/members")
    public List<PoolStore.Member> members(@PathParam("poolId") String poolId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.members(poolId);
    }

    @POST
    @Path("/{poolId}/members")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member register(@PathParam("poolId") String poolId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.registerMember(poolId, body);
    }

    @GET
    @Path("/{poolId}/members/{instanceId}")
    public PoolStore.Member member(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.member(poolId, instanceId);
    }

    @GET
    @Path("/{poolId}/members/{instanceId}/obligations")
    public PoolStore.Obligations obligations(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.obligations(poolId, instanceId);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/admit")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member admit(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.admitMember(poolId, instanceId, body);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/drain")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member drain(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.drainMember(poolId, instanceId, body);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/seal")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member seal(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.sealMember(poolId, instanceId, body);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/suspect")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member suspect(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.suspectMember(poolId, instanceId, body);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/lost")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member lost(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.lostMember(poolId, instanceId, body);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/retire")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member retire(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.retireMember(poolId, instanceId, body);
    }

    @POST
    @Path("/{poolId}/members/{instanceId}/retired")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Member retired(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.retiredMember(poolId, instanceId, body);
    }

    @GET
    @Path("/{poolId}/members/{instanceId}/failure-receipt")
    public PoolStore.FailureReceipt failureReceipt(@PathParam("poolId") String poolId, @PathParam("instanceId") String instanceId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.failureReceipt(poolId, instanceId);
    }

    @POST
    @Path("/{poolId}/publications")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Publication openPublication(@PathParam("poolId") String poolId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.openPublication(poolId, body);
    }

    @GET
    @Path("/{poolId}/publications/{publicationId}")
    public PoolStore.Publication publication(@PathParam("poolId") String poolId, @PathParam("publicationId") String publicationId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.publication(poolId, publicationId);
    }

    @POST
    @Path("/{poolId}/publications/{publicationId}/receipts")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Publication receipt(@PathParam("poolId") String poolId, @PathParam("publicationId") String publicationId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.recordPublicationReceipt(poolId, publicationId, body);
    }

    @POST
    @Path("/{poolId}/publications/{publicationId}/commit")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Publication commit(@PathParam("poolId") String poolId, @PathParam("publicationId") String publicationId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.commitPublication(poolId, publicationId, body);
    }

    @POST
    @Path("/{poolId}/publications/{publicationId}/abandon")
    @Consumes(APPLICATION_JSON)
    public PoolStore.Publication abandon(@PathParam("poolId") String poolId, @PathParam("publicationId") String publicationId, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.abandonPublication(poolId, publicationId, body);
    }

    @GET
    @Path("/{poolId}/tenants/{tenant}")
    public PoolStore.TenantAdmission tenant(@PathParam("poolId") String poolId, @PathParam("tenant") String tenant, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.tenantAdmission(poolId, tenant);
    }

    @DELETE
    @Path("/{poolId}/tenants/{tenant}")
    @Consumes(APPLICATION_JSON)
    public PoolStore.TenantAdmission revoke(@PathParam("poolId") String poolId, @PathParam("tenant") String tenant, JsonNode body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.revokeTenant(poolId, tenant, body);
    }

    @GET
    @Path("/{poolId}/operations/{operationId}")
    public PoolStore.OperationHistory operation(@PathParam("poolId") String poolId, @PathParam("operationId") String operationId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.operationHistory(poolId, operationId);
    }
}
