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
import jakarta.ws.rs.core.Response;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.gateway.ha.transaction.TransactionIdentity.error;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

final class TransactionRequestCapacity
{
    private final int limit;
    private final long requestNanos;
    private final Semaphore permits;
    private final ThreadPoolExecutor completions;
    private final AtomicBoolean stopping = new AtomicBoolean();

    TransactionRequestCapacity(TransactionAwarenessConfiguration config)
    {
        this(config, new Duration(config.getRequestTimeoutMillis(), MILLISECONDS));
    }

    TransactionRequestCapacity(TransactionAwarenessConfiguration config, Duration routingTimeout)
    {
        limit = config.getMaxInFlightRequests();
        requestNanos = MILLISECONDS.toNanos(Math.max(1, Math.min(config.getRequestTimeoutMillis(), routingTimeout.toMillis())));
        permits = new Semaphore(limit);
        completions = new ThreadPoolExecutor(
                config.getCompletionThreads(),
                config.getCompletionThreads(),
                0,
                MILLISECONDS,
                new ArrayBlockingQueue<>(limit),
                daemonThreadsNamed("transaction-completion-%s"));
    }

    Lease acquire()
    {
        if (stopping.get() || !permits.tryAcquire()) {
            throw overloaded();
        }
        if (stopping.get()) {
            release();
            throw overloaded();
        }
        return new Lease(System.nanoTime() + requestNanos);
    }

    private static WebApplicationException overloaded()
    {
        return new WebApplicationException(Response.status(503).header("Retry-After", "1")
                .entity("Transaction-aware request capacity is unavailable; no backend request was dispatched").build());
    }

    Executor completions()
    {
        return completions;
    }

    void shutdown()
    {
        stopping.set(true);
        stopWhenDrained();
    }

    private void release()
    {
        permits.release();
        stopWhenDrained();
    }

    private void stopWhenDrained()
    {
        if (stopping.get() && permits.availablePermits() == limit) {
            completions.shutdown();
        }
    }

    final class Lease
            implements AutoCloseable
    {
        private final long deadlineNanos;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean dispatched = new AtomicBoolean();
        private final AtomicBoolean completionManaged = new AtomicBoolean();

        private Lease(long deadlineNanos)
        {
            this.deadlineNanos = deadlineNanos;
        }

        long deadlineNanos()
        {
            return deadlineNanos;
        }

        Duration remaining()
        {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw error(504, "Transaction-aware request deadline expired before dispatch");
            }
            return new Duration(remaining, NANOSECONDS);
        }

        void dispatch()
        {
            remaining();
            if (!dispatched.compareAndSet(false, true)) {
                throw new IllegalStateException("A request must not be dispatched twice");
            }
        }

        boolean isDispatched()
        {
            return dispatched.get();
        }

        void manageCompletion()
        {
            completionManaged.set(true);
        }

        boolean isCompletionManaged()
        {
            return completionManaged.get();
        }

        @Override
        public void close()
        {
            if (closed.compareAndSet(false, true)) {
                release();
            }
        }
    }
}
