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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The restriction's lookup key must be a name the coordinator could actually authenticate. These
 * assertions pin the parsing and qualification rules; which tenant a name belongs to is not decided
 * here at all, it comes from the controller-published mapping.
 */
class TestTenantPrincipals
{
    private static final String DOMAIN = "dw.example.test";
    private static final List<String> DOMAINS = List.of(DOMAIN);
    private static final Set<String> EXCLUDED = Set.of("internal", "admin");

    @Test
    void aCredentialSentToATenantHostIsQualifiedAndNothingElse()
    {
        // The coordinator qualifies before it authenticates and checks only this name. The name as
        // typed is deliberately not an alternative: treating it as one would let a request whose
        // qualified principal is unpublished pass on an unrelated mapping for the typed name.
        assertThat(principal("alice", "warehouse-one." + DOMAIN)).contains("warehouse-one.alice");
    }

    @Test
    void aCredentialAtTheApexHostYieldsOnlyItself()
    {
        // A tenant's root login is its bare warehouse name, and at a host that does not qualify the
        // coordinator authenticates it exactly as presented.
        assertThat(principal("warehouse-one", DOMAIN)).contains("warehouse-one");
    }

    @Test
    void aMultiLabelHostDoesNotQualify()
    {
        assertThat(principal("alice", "a.b." + DOMAIN)).contains("alice");
    }

    @Test
    void aHostOutsideTheConfiguredDomainDoesNotQualify()
    {
        assertThat(principal("alice", "warehouse-one.other.test")).contains("alice");
    }

    @Test
    void anExcludedHostLabelDoesNotQualify()
    {
        // The coordinator excludes operational host names, so neither side reads them as a tenant.
        assertThat(principal("alice", "internal." + DOMAIN)).contains("alice");
        assertThat(principal("alice", "admin." + DOMAIN)).contains("alice");
    }

    @Test
    void anAlreadyQualifiedCredentialIsQualifiedAgainExactlyAsTheCoordinatorWould()
    {
        // The coordinator prefixes whatever it was given, so the doubly qualified name is what it
        // authenticates. Whether it is published is the mapping's business, not this class's.
        assertThat(principal("warehouse-one.alice", "warehouse-two." + DOMAIN)).contains("warehouse-two.warehouse-one.alice");
    }

    @Test
    void anInvalidHostLabelDoesNotQualify()
    {
        assertThat(principal("alice", "-nope." + DOMAIN)).contains("alice");
        assertThat(principal("alice", "UPPER." + DOMAIN)).contains("upper.alice");
    }

    @Test
    void aNestedDomainQualifiesUnderItsLongestMatchingSuffix()
    {
        // With both a domain and a nested domain configured, a host that is one label under the nested
        // one qualifies there. The same host is two labels under the shorter suffix, which does not
        // qualify, so the order the domains are configured in must not change the answer.
        List<String> nested = List.of(DOMAIN, "cell." + DOMAIN);
        assertThat(principal("alice", "warehouse-one.cell." + DOMAIN, nested)).contains("warehouse-one.alice");
        assertThat(principal("alice", "warehouse-one.cell." + DOMAIN, List.of("cell." + DOMAIN, DOMAIN))).contains("warehouse-one.alice");
        // A host directly under the shorter domain still qualifies there.
        assertThat(principal("alice", "warehouse-one." + DOMAIN, nested)).contains("warehouse-one.alice");
    }

    @Test
    void aTrailingDotAndAPortDoNotChangeTheCandidates()
    {
        assertThat(principal("alice", "warehouse-one." + DOMAIN + ".")).contains("warehouse-one.alice");
        assertThat(principal("alice", "warehouse-one." + DOMAIN + ":8443")).contains("warehouse-one.alice");
    }

    @Test
    void noConfiguredDomainLeavesTheCredentialAsTheOnlyCandidate()
    {
        assertThat(principal("alice", "warehouse-one." + DOMAIN, List.of())).contains("alice");
    }

    @Test
    void aRequestUserHeaderIsNeverACandidate()
    {
        // A user header is not evidence of authentication. Selection and impersonation stay with the
        // coordinator and its policy.
        Optional<String> principal = TenantPrincipals.authenticatedPrincipal(
                request(Map.of(
                        "Authorization", List.of(basic("alice", "secret")),
                        "Host", List.of("warehouse-one." + DOMAIN),
                        "X-Trino-User", List.of("warehouse-two.mallory"),
                        "X-Trino-Original-User", List.of("warehouse-two.mallory"))),
                DOMAINS,
                EXCLUDED);
        assertThat(principal).contains("warehouse-one.alice");
    }

    @Test
    void theCredentialIsDecodedAsIsoLatinOneLikeTheCoordinator()
    {
        // This byte is one ISO-8859-1 character and a malformed UTF-8 sequence. Decoding it as UTF-8
        // would key the restriction on a different string than the coordinator authenticates.
        byte[] credential = "café:secret".getBytes(ISO_8859_1);
        assertThat(authorized("Basic " + Base64.getEncoder().encodeToString(credential), DOMAIN, DOMAINS)).contains("café");
        assertThat(new String(credential, UTF_8)).isNotEqualTo(new String(credential, ISO_8859_1));
    }

    @Test
    void aBearerCredentialNeverKeysTheRestriction()
    {
        assertThat(authorized("Bearer some.jwt.value", "warehouse-one." + DOMAIN, DOMAINS)).isEmpty();
    }

    @Test
    void aMalformedOrEmptyUserCredentialIsRefused()
    {
        assertThatThrownBy(() -> authorized("Basic not-base64!!", DOMAIN, DOMAINS))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(401));
        assertThatThrownBy(() -> principal("", DOMAIN))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(401));
    }

    @Test
    void duplicateIdentityHeadersAreRefusedRatherThanCoalesced()
    {
        assertThatThrownBy(() -> TenantPrincipals.authenticatedPrincipal(
                request(Map.of("Authorization", List.of(basic("alice", "a"), basic("bob", "b")), "Host", List.of(DOMAIN))),
                DOMAINS,
                EXCLUDED))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(400));
        assertThatThrownBy(() -> TenantPrincipals.authenticatedPrincipal(
                request(Map.of("Authorization", List.of(basic("alice", "a")), "Host", List.of(DOMAIN, "other." + DOMAIN))),
                DOMAINS,
                EXCLUDED))
                .isInstanceOfSatisfying(WebApplicationException.class, failure -> assertThat(failure.getResponse().getStatus()).isEqualTo(400));
    }

    @Test
    void aClientSuppliedForwardedHostNeverChangesTheCandidates()
    {
        Optional<String> principal = TenantPrincipals.authenticatedPrincipal(
                request(Map.of(
                        "Authorization", List.of(basic("alice", "secret")),
                        "Host", List.of("warehouse-one." + DOMAIN),
                        "X-Forwarded-Host", List.of("warehouse-nine." + DOMAIN),
                        "Forwarded", List.of("host=warehouse-nine." + DOMAIN))),
                DOMAINS,
                EXCLUDED);
        assertThat(principal).contains("warehouse-one.alice");
        assertThat(TenantPrincipals.isForwardedHeader("X-Forwarded-Host")).isTrue();
        assertThat(TenantPrincipals.isForwardedHeader("forwarded")).isTrue();
        assertThat(TenantPrincipals.isForwardedHeader("Host")).isFalse();
    }

    private static Optional<String> principal(String user, String host)
    {
        return principal(user, host, DOMAINS);
    }

    private static Optional<String> principal(String user, String host, List<String> domains)
    {
        return authorized(basic(user, "secret"), host, domains);
    }

    private static Optional<String> authorized(String authorization, String host, List<String> domains)
    {
        return TenantPrincipals.authenticatedPrincipal(
                request(Map.of("Authorization", List.of(authorization), "Host", List.of(host))),
                domains,
                EXCLUDED);
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
