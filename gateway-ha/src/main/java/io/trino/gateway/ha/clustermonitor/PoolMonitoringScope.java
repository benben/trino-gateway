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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.BackendStateManager;
import io.trino.gateway.ha.router.RoutingManager;
import io.trino.gateway.ha.transaction.PoolLifecycleService;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Removes irreversibly retired and proven-lost pool members from monitoring selection and drops
 * their cached health and state entries, so historical registrations cannot make polling grow
 * forever. Transaction, query and admission tombstones stay in the database under their own
 * retention policy — only the in-memory monitoring cost is released.
 */
@Singleton
public class PoolMonitoringScope
{
    private static final Logger log = Logger.get(PoolMonitoringScope.class);

    private final PoolLifecycleService pools;
    private final RoutingManager routingManager;
    private final BackendStateManager backendStateManager;

    @Inject
    public PoolMonitoringScope(PoolLifecycleService pools, RoutingManager routingManager, BackendStateManager backendStateManager)
    {
        this.pools = requireNonNull(pools, "pools is null");
        this.routingManager = requireNonNull(routingManager, "routingManager is null");
        this.backendStateManager = requireNonNull(backendStateManager, "backendStateManager is null");
    }

    /**
     * Returns the backends that should still be polled, releasing cached entries for the rest.
     * With the pooled lifecycle disabled the input list is returned unchanged.
     */
    public List<ProxyBackendConfiguration> monitored(List<ProxyBackendConfiguration> backends)
    {
        Set<String> retired = pools.unmonitoredBackendNames();
        if (retired.isEmpty()) {
            return backends;
        }
        for (ProxyBackendConfiguration backend : backends) {
            if (retired.contains(backend.getName())) {
                release(backend.getName());
            }
        }
        return backends.stream().filter(backend -> !retired.contains(backend.getName())).toList();
    }

    private void release(String name)
    {
        try {
            if (routingManager.getBackEndHealth(name).isPresent()) {
                log.info("Releasing monitoring state for retired pool member '%s'", name);
            }
            routingManager.removeBackEndHealth(name);
            backendStateManager.removeStates(name);
        }
        catch (RuntimeException e) {
            log.warn(e, "Could not release monitoring state for retired pool member '%s'", name);
        }
    }
}
