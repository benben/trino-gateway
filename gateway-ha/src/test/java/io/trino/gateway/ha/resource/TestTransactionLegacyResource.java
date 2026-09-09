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

import io.trino.gateway.ha.router.BackendLifecycleManager;
import io.trino.gateway.ha.router.GatewayBackendManager;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

final class TestTransactionLegacyResource
{
    private final BackendLifecycleManager lifecycle = mock(BackendLifecycleManager.class);
    private final GatewayResource resource = new GatewayResource(mock(GatewayBackendManager.class), lifecycle);

    @Test
    void testActivatePreservesTransactionConflict()
    {
        Response conflict = Response.status(409).entity("Use durable lifecycle operations").type("text/plain").build();
        doThrow(new WebApplicationException(conflict)).when(lifecycle).activateBackend("blue");

        assertThat(resource.activateBackend("blue")).isSameAs(conflict);
    }

    @Test
    void testDeactivatePreservesTransactionConflict()
    {
        Response conflict = Response.status(409).entity("Use durable lifecycle operations").type("text/plain").build();
        doThrow(new WebApplicationException(conflict)).when(lifecycle).deactivateBackend("blue");

        assertThat(resource.deactivateBackend("blue")).isSameAs(conflict);
    }

    @Test
    void testMissingActivationRetainsLegacyNotFound()
    {
        doThrow(new IllegalStateException("No cluster found")).when(lifecycle).activateBackend("missing");

        Response response = resource.activateBackend("missing");
        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getEntity()).isEqualTo("No cluster found");
    }

    @Test
    void testMissingDeactivationRetainsLegacyNotFound()
    {
        doThrow(new IllegalStateException("No cluster found")).when(lifecycle).deactivateBackend("missing");

        Response response = resource.deactivateBackend("missing");
        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getEntity()).isEqualTo("No cluster found");
    }
}
