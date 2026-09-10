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

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDatabaseDeadline
{
    @Test
    void nestedScopeCannotExtendExpiredDeadlineOrRemoveAdmissionReservation()
            throws Exception
    {
        DatabaseDeadline.withDeadline(System.nanoTime() - 1, true, () -> {
            DatabaseDeadline.withDeadline(System.nanoTime() + TimeUnit.SECONDS.toNanos(10), false, () -> {
                assertThat(DatabaseDeadline.isAdmission()).isTrue();
                assertThatThrownBy(DatabaseDeadline::remainingNanos).isInstanceOf(SQLException.class);
                return null;
            });
            assertThat(DatabaseDeadline.isAdmission()).isTrue();
            return null;
        });
        assertThat(DatabaseDeadline.isAdmission()).isFalse();
        assertThat(DatabaseDeadline.remainingNanos()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void exceptionRestoresEarlierContextAndDoesNotLeakToNextRequest()
            throws Exception
    {
        DatabaseDeadline.withDeadline(System.nanoTime() + TimeUnit.SECONDS.toNanos(10), false, () -> {
            assertThatThrownBy(() -> DatabaseDeadline.withDeadline(System.nanoTime() - 1, true, () -> {
                throw new IllegalStateException("synthetic failure");
            })).isInstanceOf(IllegalStateException.class);
            assertThat(DatabaseDeadline.isAdmission()).isFalse();
            return null;
        });
        assertThat(DatabaseDeadline.remainingNanos()).isEqualTo(Long.MAX_VALUE);
    }
}
