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
package io.trino.gateway.ha.persistence;

import java.sql.SQLException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class DatabaseDeadline
{
    private record Context(long deadlineNanos, boolean admission) {}

    private static final ThreadLocal<Context> CURRENT = new ThreadLocal<>();

    private DatabaseDeadline() {}

    public static <T> T withDeadline(long deadlineNanos, boolean admission, Supplier<T> action)
    {
        requireNonNull(action, "action is null");
        Context previous = CURRENT.get();
        Context current = previous == null ? new Context(deadlineNanos, admission) :
                new Context(Math.min(previous.deadlineNanos(), deadlineNanos), previous.admission() || admission);
        CURRENT.set(current);
        try {
            return action.get();
        }
        finally {
            if (previous == null) {
                CURRENT.remove();
            }
            else {
                CURRENT.set(previous);
            }
        }
    }

    static boolean isAdmission()
    {
        Context context = CURRENT.get();
        return context != null && context.admission();
    }

    static long remainingNanos()
            throws SQLException
    {
        Context context = CURRENT.get();
        if (context == null) {
            return Long.MAX_VALUE;
        }
        long remaining = context.deadlineNanos() - System.nanoTime();
        if (remaining <= 0) {
            throw new SQLException("Gateway database phase deadline expired", "57014");
        }
        return remaining;
    }
}
