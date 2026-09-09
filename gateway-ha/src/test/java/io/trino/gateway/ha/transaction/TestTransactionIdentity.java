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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestTransactionIdentity
{
    private final TransactionIdentity identity = new TransactionIdentity("synthetic-identity-key-for-unit-tests-only");

    @Test
    void testStableCanonicalCredentialAndUserBinding()
    {
        String owner = identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password")))));
        assertThat(owner).hasSize(129).doesNotContain("alice", "password");
        assertThat(identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password")), "X-Trino-User", List.of("alice"), "X-Trino-Original-User", List.of("alice"))))).isEqualTo(owner);
        assertThat(identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password").replace("Basic", "basic")))))).isEqualTo(owner);
    }

    @Test
    void testCredentialAndUserChangesDoNotShareOwnership()
    {
        String owner = identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password")))));
        assertThat(identity.owner(request(Map.of("Authorization", List.of(basic("alice", "other")))))).isNotEqualTo(owner);
        assertThat(identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password")), "X-Trino-User", List.of("bob"))))).isNotEqualTo(owner);
        assertThat(identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password")), "X-Trino-Original-User", List.of("bob"))))).isNotEqualTo(owner);
    }

    @Test
    void testHeaderlessAndCredentialOnlyContinuations()
    {
        String owner = identity.owner(request(Map.of("Authorization", List.of(basic("service", "password")), "X-Trino-User", List.of("tenant"))));
        identity.validateContinuation(request(Map.of()), owner);
        identity.validateContinuation(request(Map.of("Authorization", List.of(basic("service", "password")))), owner);
        identity.validateContinuation(request(Map.of("Authorization", List.of(basic("service", "password")), "X-Trino-User", List.of("tenant"))), owner);
        assertThatThrownBy(() -> identity.validateContinuation(request(Map.of("Authorization", List.of(basic("service", "wrong")))), owner)).isInstanceOf(WebApplicationException.class);
        assertThatThrownBy(() -> identity.validateContinuation(request(Map.of("Authorization", List.of(basic("service", "password")), "X-Trino-User", List.of("other"))), owner)).isInstanceOf(WebApplicationException.class);
    }

    @Test
    void testUnverifiedUserHeaderIsNotAnIdentity()
    {
        assertThatThrownBy(() -> identity.owner(request(Map.of("X-Trino-User", List.of("alice"))))).isInstanceOf(WebApplicationException.class);
        assertThatThrownBy(() -> identity.validateContinuation(request(Map.of("X-Trino-User", List.of("alice"))), "unused")).isInstanceOf(WebApplicationException.class);
    }

    @Test
    void testMalformedMissingAndDuplicateAuthorization()
    {
        for (List<String> authorization : List.of(List.<String>of(), List.of(""), List.of("Bearer unverified"), List.of("Basic !!!"), List.of("Basic " + Base64.getEncoder().encodeToString("missing-colon".getBytes(UTF_8))), List.of(basic("alice", "password"), basic("alice", "password")))) {
            assertThatThrownBy(() -> identity.owner(request(Map.of("Authorization", authorization)))).isInstanceOf(WebApplicationException.class);
        }
    }

    @Test
    void testDuplicateContextHeadersAreRejected()
    {
        assertThatThrownBy(() -> identity.owner(request(Map.of("Authorization", List.of(basic("alice", "password")), "X-Trino-User", List.of("alice", "alice"))))).isInstanceOf(WebApplicationException.class);
    }

    @Test
    void testTransactionHeaderValidation()
    {
        String transaction = "01234567-89ab-cdef-0123-456789abcdef";
        assertThat(TransactionIdentity.transactionId(request(Map.of()))).isEmpty();
        assertThat(TransactionIdentity.transactionId(request(Map.of("X-Trino-Transaction-Id", List.of("NONE"))))).isEmpty();
        assertThat(TransactionIdentity.transactionId(request(Map.of("x-trino-transaction-id", List.of(transaction.toUpperCase(java.util.Locale.ENGLISH)))))).contains(transaction);
        for (List<String> values : List.of(List.of(""), List.of("none"), List.of("1-1-1-1-1"), List.of(transaction, transaction), List.of(transaction + "," + transaction))) {
            assertThatThrownBy(() -> TransactionIdentity.transactionId(request(Map.of("X-Trino-Transaction-Id", values)))).isInstanceOf(WebApplicationException.class);
        }
    }

    @Test
    void testDifferentSharedKeysCannotReadBindings()
    {
        HttpServletRequest request = request(Map.of("Authorization", List.of(basic("alice", "password"))));
        assertThat(new TransactionIdentity("another-synthetic-key-for-unit-tests").owner(request)).isNotEqualTo(identity.owner(request));
    }

    private static String basic(String user, String password)
    {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(UTF_8));
    }

    private static HttpServletRequest request(Map<String, List<String>> headers)
    {
        TreeMap<String, List<String>> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        normalized.putAll(headers);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeaders(anyString())).thenAnswer(invocation -> Collections.enumeration(normalized.getOrDefault(invocation.getArgument(0), List.of())));
        return request;
    }
}
