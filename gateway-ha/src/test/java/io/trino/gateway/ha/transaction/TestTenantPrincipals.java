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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The restriction's key must be the same string the coordinator authenticates. These assertions pin
 * the parsing rules that equality depends on.
 */
class TestTenantPrincipals
{
    private static final String DOMAIN = "dw.example.test";

    @Test
    void aHostQualifiedCredentialNamesItsWarehouseAsTheTenant()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("alice", "secret")), "Host", List.of("tenant-a." + DOMAIN))), DOMAIN);
        assertThat(candidates.principals()).containsExactlyInAnyOrder("alice", "tenant-a.alice");
        assertThat(candidates.tenants()).containsExactly("tenant-a");
    }

    @Test
    void aBareCredentialAtTheApexHostNamesNoTenant()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("alice", "secret")), "Host", List.of(DOMAIN))), DOMAIN);
        assertThat(candidates.principals()).containsExactly("alice");
        assertThat(candidates.tenants()).isEmpty();
    }

    @Test
    void aMultiLabelHostIsNotQualified()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("alice", "secret")), "Host", List.of("a.b." + DOMAIN))), DOMAIN);
        assertThat(candidates.tenants()).isEmpty();
    }

    @Test
    void anAlreadyQualifiedCredentialIsNotQualifiedTwice()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("tenant-a.alice", "secret")), "Host", List.of("tenant-b." + DOMAIN))), DOMAIN);
        assertThat(candidates.principals()).containsExactly("tenant-a.alice");
        assertThat(candidates.tenants()).containsExactly("tenant-a");
    }

    @Test
    void everyIdentityHeaderContributesACandidate()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of(
                        "Authorization", List.of(basic("alice", "secret")),
                        "Host", List.of("tenant-a." + DOMAIN),
                        "X-Trino-User", List.of("tenant-b.alice"),
                        "X-Trino-Original-User", List.of("tenant-c.alice"))),
                DOMAIN);
        assertThat(candidates.tenants()).containsExactlyInAnyOrder("tenant-a", "tenant-b", "tenant-c");
    }

    @Test
    void theCredentialIsDecodedAsIsoLatinOneLikeTheCoordinator()
    {
        // 0xE9 is a single ISO-8859-1 character and a malformed UTF-8 sequence. Decoding it as UTF-8
        // would key the restriction on a different string than the one the coordinator authenticates.
        byte[] credential = "café.alice:secret".getBytes(ISO_8859_1);
        String header = "Basic " + Base64.getEncoder().encodeToString(credential);
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(header), "Host", List.of(DOMAIN))), DOMAIN);
        assertThat(candidates.principals()).containsExactly("café.alice");
        assertThat(candidates.tenants()).containsExactly("café");
        assertThat(new String(credential, UTF_8)).isNotEqualTo(new String(credential, ISO_8859_1));
    }

    @Test
    void aBearerCredentialNeverKeysTheRestriction()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of("Bearer some.jwt.value"), "Host", List.of("tenant-a." + DOMAIN))), DOMAIN);
        assertThat(candidates.principals()).isEmpty();
        assertThat(candidates.tenants()).isEmpty();
    }

    @Test
    void aMalformedOrEmptyUserCredentialIsRefused()
    {
        assertThatThrownBy(() -> TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of("Basic not-base64!!"), "Host", List.of(DOMAIN))), DOMAIN))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(401));
        assertThatThrownBy(() -> TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("", "secret")), "Host", List.of(DOMAIN))), DOMAIN))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(401));
    }

    @Test
    void duplicateIdentityHeadersAreRefusedRatherThanCoalesced()
    {
        assertThatThrownBy(() -> TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("alice", "a"), basic("bob", "b")), "Host", List.of(DOMAIN))), DOMAIN))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(400));
        assertThatThrownBy(() -> TenantPrincipals.candidates(
                request(Map.of(
                        "Authorization", List.of(basic("alice", "a")),
                        "Host", List.of(DOMAIN),
                        "X-Trino-User", List.of("one", "two"))),
                DOMAIN))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(400));
    }

    @Test
    void aClientSuppliedForwardedHostNeverChangesTheKey()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of(
                        "Authorization", List.of(basic("alice", "secret")),
                        "Host", List.of("tenant-a." + DOMAIN),
                        "X-Forwarded-Host", List.of("tenant-b." + DOMAIN),
                        "Forwarded", List.of("host=tenant-c." + DOMAIN))),
                DOMAIN);
        assertThat(candidates.tenants()).containsExactly("tenant-a");
        assertThat(TenantPrincipals.isForwardedHeader("X-Forwarded-Host")).isTrue();
        assertThat(TenantPrincipals.isForwardedHeader("forwarded")).isTrue();
        assertThat(TenantPrincipals.isForwardedHeader("Host")).isFalse();
    }

    @Test
    void thePortAndCaseOfTheRoutedHostDoNotChangeTheKey()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("alice", "secret")), "Host", List.of("Tenant-A." + DOMAIN + ":8443"))), DOMAIN);
        assertThat(candidates.tenants()).containsExactly("tenant-a");
    }

    @Test
    void noQualificationDomainMeansNoQualifiedCandidate()
    {
        TenantPrincipals.Candidates candidates = TenantPrincipals.candidates(
                request(Map.of("Authorization", List.of(basic("alice", "secret")), "Host", List.of("tenant-a." + DOMAIN))), null);
        assertThat(candidates.principals()).containsExactly("alice");
        assertThat(candidates.tenants()).isEmpty();
    }

    private static String basic(String user, String password)
    {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(ISO_8859_1));
    }

    private static HttpServletRequest request(Map<String, List<String>> suppliedHeaders)
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.putAll(suppliedHeaders);
        when(request.getHeaders(anyString())).thenAnswer(call -> Collections.enumeration(headers.getOrDefault(call.getArgument(0), List.of())));
        when(request.getHeader(anyString())).thenAnswer(call -> headers.getOrDefault(call.getArgument(0), List.of()).stream().findFirst().orElse(null));
        when(request.getHeaderNames()).thenAnswer(_ -> Collections.enumeration(headers.keySet()));
        return request;
    }
}
