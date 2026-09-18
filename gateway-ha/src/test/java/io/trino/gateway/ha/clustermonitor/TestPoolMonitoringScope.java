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
package io.trino.gateway.ha.clustermonitor;

import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.BackendStateManager;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.transaction.PoolLifecycleService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestPoolMonitoringScope
{
    @Test
    void retiredAndLostMembersLeaveMonitoringAndReleaseTheirCachedEntries()
    {
        PoolLifecycleService pools = mock(PoolLifecycleService.class);
        RoutingManager routingManager = mock(RoutingManager.class);
        BackendStateManager backendStateManager = mock(BackendStateManager.class);
        when(pools.unmonitoredBackendNames()).thenReturn(Set.of("retired", "lost"));
        when(routingManager.getBackEndHealth(anyString())).thenReturn(Optional.of(TrinoStatus.HEALTHY));

        List<ProxyBackendConfiguration> monitored = new PoolMonitoringScope(pools, routingManager, backendStateManager)
                .monitored(List.of(backend("serving"), backend("retired"), backend("lost")));

        assertThat(monitored).extracting(ProxyBackendConfiguration::getName).containsExactly("serving");
        verify(routingManager).removeBackEndHealth("retired");
        verify(routingManager).removeBackEndHealth("lost");
        verify(routingManager, never()).removeBackEndHealth("serving");
        verify(backendStateManager).removeStates("retired");
        verify(backendStateManager).removeStates("lost");
        verify(backendStateManager, never()).removeStates("serving");
    }

    @Test
    void monitoringIsUntouchedWhenNoMemberHasLeftService()
    {
        PoolLifecycleService pools = mock(PoolLifecycleService.class);
        RoutingManager routingManager = mock(RoutingManager.class);
        BackendStateManager backendStateManager = mock(BackendStateManager.class);
        when(pools.unmonitoredBackendNames()).thenReturn(Set.of());

        List<ProxyBackendConfiguration> backends = List.of(backend("blue"), backend("green"));
        assertThat(new PoolMonitoringScope(pools, routingManager, backendStateManager).monitored(backends)).isSameAs(backends);
        verifyNoInteractions(routingManager);
        verifyNoInteractions(backendStateManager);
    }

    @Test
    void anUnreadableCleanupDoesNotStopMonitoringTheRemainingClusters()
    {
        PoolLifecycleService pools = mock(PoolLifecycleService.class);
        RoutingManager routingManager = mock(RoutingManager.class);
        BackendStateManager backendStateManager = mock(BackendStateManager.class);
        when(pools.unmonitoredBackendNames()).thenReturn(Set.of("retired"));
        when(routingManager.getBackEndHealth("retired")).thenThrow(new IllegalStateException("synthetic failure"));

        List<ProxyBackendConfiguration> monitored = new PoolMonitoringScope(pools, routingManager, backendStateManager)
                .monitored(List.of(backend("serving"), backend("retired")));

        assertThat(monitored).extracting(ProxyBackendConfiguration::getName).containsExactly("serving");
    }

    private static ProxyBackendConfiguration backend(String name)
    {
        ProxyBackendConfiguration backend = new ProxyBackendConfiguration();
        backend.setName(name);
        backend.setProxyTo("http://" + name + ".example.test");
        backend.setRoutingGroup("pool-a");
        backend.setActive(true);
        return backend;
    }
}
