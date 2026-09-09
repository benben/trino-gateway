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

import com.google.inject.Inject;
import io.trino.gateway.ha.transaction.TransactionAwarenessService;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

import java.util.Map;
import java.util.UUID;

import static io.trino.gateway.ha.transaction.TransactionIdentity.error;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/gateway/transactions")
@Produces(APPLICATION_JSON)
@RolesAllowed("API")
public class TransactionResource
{
    private final TransactionAwarenessService service;

    @Inject
    public TransactionResource(TransactionAwarenessService service)
    {
        this.service = service;
    }

    @GET
    @Path("/{transactionId}")
    public Map<String, Object> transaction(@PathParam("transactionId") String transactionId, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.transaction(transactionId);
    }

    @GET
    @Path("/backends/{name}/drain")
    public Map<String, Object> status(@PathParam("name") String name, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.drain(name, false);
    }

    @POST
    @Path("/backends/{name}/drain")
    public Map<String, Object> drain(@PathParam("name") String name, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.drain(name, true);
    }

    @POST
    @Path("/backends/{name}/seal")
    @Consumes(APPLICATION_JSON)
    public Map<String, Object> seal(@PathParam("name") String name, Map<String, Long> body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.seal(name, generation(body));
    }

    @POST
    @Path("/backends/{name}/resume")
    @Consumes(APPLICATION_JSON)
    public Map<String, Object> resume(@PathParam("name") String name, Map<String, Long> body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.resume(name, generation(body));
    }

    @POST
    @Path("/cutover")
    @Consumes(APPLICATION_JSON)
    public Map<String, Object> cutover(Map<String, String> body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        if (body == null || body.get("routingGroup") == null || body.get("backendName") == null) {
            throw error(400, "routingGroup and backendName are required");
        }
        return service.cutover(body.get("routingGroup"), body.get("backendName"));
    }

    @POST
    @Path("/backends/{name}/reincarnate")
    @Consumes(APPLICATION_JSON)
    public Map<String, Object> reincarnate(@PathParam("name") String name, ReincarnationRequest body, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        if (body == null || body.incarnation() == null || body.generation() == null || body.generation() < 0) {
            throw error(400, "The observed incarnation and generation are required");
        }
        return service.reincarnate(name, body.incarnation(), body.generation());
    }

    public record ReincarnationRequest(UUID incarnation, Long generation) {}

    @DELETE
    @Path("/cutover/{routingGroup}")
    public Map<String, Object> clearRoute(@PathParam("routingGroup") String routingGroup, @Context HttpServletRequest request)
    {
        service.requireAdmin(request);
        return service.clearRoute(routingGroup);
    }

    private static long generation(Map<String, Long> body)
    {
        if (body == null || body.get("generation") == null || body.get("generation") < 0) {
            throw error(400, "The observed backend generation is required");
        }
        return body.get("generation");
    }
}
