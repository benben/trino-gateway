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

import io.airlift.units.Duration;
import io.trino.gateway.ha.config.TransactionAwarenessConfiguration;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTransactionRequestCapacity
{
    @Test
    void completionWorkersAndWaitingBodiesRemainBoundedDuringShutdown()
            throws Exception
    {
        TransactionRequestCapacity capacity = new TransactionRequestCapacity(new TransactionAwarenessConfiguration());
        List<TransactionRequestCapacity.Lease> leases = new ArrayList<>();
        CountDownLatch running = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(16);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        try {
            for (int index = 0; index < 16; index++) {
                leases.add(capacity.acquire());
            }
            capacity.shutdown();
            assertOverloaded(capacity);
            for (TransactionRequestCapacity.Lease lease : leases) {
                capacity.completions().execute(() -> {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    running.countDown();
                    try {
                        assertThat(release.await(5, SECONDS)).isTrue();
                    }
                    catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(failure);
                    }
                    finally {
                        active.decrementAndGet();
                        lease.close();
                        finished.countDown();
                    }
                });
            }
            assertThat(running.await(5, SECONDS)).isTrue();
            assertThat(finished.getCount()).isEqualTo(16);
            assertThat(peak.get()).isEqualTo(4);
        }
        finally {
            release.countDown();
        }
        assertThat(finished.await(5, SECONDS)).isTrue();
        assertThat(peak.get()).isEqualTo(4);
        assertOverloaded(capacity);
    }

    @Test
    void duplicateReleaseCannotIncreaseCapacity()
    {
        TransactionAwarenessConfiguration config = new TransactionAwarenessConfiguration();
        config.setMaxInFlightRequests(1);
        config.setCompletionThreads(1);
        TransactionRequestCapacity capacity = new TransactionRequestCapacity(config);
        TransactionRequestCapacity.Lease first = capacity.acquire();
        first.close();
        first.close();
        try (TransactionRequestCapacity.Lease second = capacity.acquire()) {
            assertOverloaded(capacity);
            second.dispatch();
            assertThatThrownBy(second::dispatch).isInstanceOf(IllegalStateException.class);
        }
        capacity.shutdown();
    }

    @Test
    void routingDeadlineCanBeShorterThanFeatureDeadline()
            throws Exception
    {
        TransactionRequestCapacity capacity = new TransactionRequestCapacity(new TransactionAwarenessConfiguration(), new Duration(5, MILLISECONDS));
        try (TransactionRequestCapacity.Lease lease = capacity.acquire()) {
            MILLISECONDS.sleep(20);
            assertThatThrownBy(lease::dispatch).isInstanceOfSatisfying(
                    WebApplicationException.class,
                    failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(504));
            assertThat(lease.isDispatched()).isFalse();
        }
        capacity.shutdown();
    }

    private static void assertOverloaded(TransactionRequestCapacity capacity)
    {
        assertThatThrownBy(capacity::acquire).isInstanceOfSatisfying(
                WebApplicationException.class,
                failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(503));
    }
}
