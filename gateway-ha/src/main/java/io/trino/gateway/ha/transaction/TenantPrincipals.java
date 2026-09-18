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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.gateway.ha.transaction.TransactionIdentity.error;
import static io.trino.gateway.ha.transaction.TransactionIdentity.singleHeader;

/**
 * Derives the tenant names a request could possibly execute as, for the deny-only pooled admission
 * restriction.
 * <p>
 * This is deliberately <em>not</em> authentication. The coordinator verifies the forwarded Basic
 * credential itself on every statement, and its authorization policy refuses impersonation, so a
 * claimed name can never become an authenticated principal here. The restriction only refuses to
 * dispatch work whose claimed tenant is not admitted yet; absence of a claim grants nothing.
 * <p>
 * Parsing mirrors the coordinator byte for byte, because a gate keyed on a different string than the
 * one the coordinator authenticates would be trivially bypassable:
 * <ul>
 *   <li>the credential is decoded as ISO-8859-1, not UTF-8;</li>
 *   <li>the scheme is matched as an exact case-insensitive {@code "Basic "} prefix;</li>
 *   <li>the decoded value is split on its first colon and an empty user is refused;</li>
 *   <li>a duplicate or empty identity header is refused rather than silently coalesced;</li>
 *   <li>host qualification is recomputed from the same inputs, so every name the coordinator could
 *       authenticate appears as a candidate.</li>
 * </ul>
 */
public final class TenantPrincipals
{
    private TenantPrincipals() {}

    /**
     * Every name this request could be authenticated as, together with the tenants those names
     * belong to. A request is refused when any known candidate tenant is not admitted, or when no
     * candidate names an admitted tenant at all.
     */
    public record Candidates(Set<String> principals, Set<String> tenants) {}

    /**
     * @param hostQualificationDomain the domain under which the coordinator qualifies a bare user
     *         with the request host's single leading label, exactly as the coordinator's password
     *         authenticator does before verifying the credential.
     */
    public static Candidates candidates(HttpServletRequest request, String hostQualificationDomain)
    {
        Set<String> principals = new LinkedHashSet<>();
        basicUser(request).ifPresent(user -> {
            principals.add(user);
            qualify(user, request, hostQualificationDomain).ifPresent(principals::add);
        });
        singleHeader(request, "X-Trino-User").filter(value -> !value.isBlank()).ifPresent(principals::add);
        singleHeader(request, "X-Trino-Original-User").filter(value -> !value.isBlank()).ifPresent(principals::add);

        Set<String> tenants = new LinkedHashSet<>();
        for (String principal : principals) {
            tenantOf(principal).ifPresent(tenants::add);
        }
        return new Candidates(Set.copyOf(principals), Set.copyOf(tenants));
    }

    /**
     * The tenant a principal belongs to: the leading label of a qualified principal. A bare name
     * carries no tenant, so it can never satisfy the restriction on its own.
     */
    public static Optional<String> tenantOf(String principal)
    {
        int separator = principal.indexOf('.');
        if (separator < 1 || separator == principal.length() - 1) {
            return Optional.empty();
        }
        return Optional.of(principal.substring(0, separator));
    }

    /**
     * Recomputes the coordinator's host qualification: a request host of exactly one label under the
     * configured domain qualifies the user as {@code <label>.<user>}.
     */
    public static Optional<String> qualify(String user, HttpServletRequest request, String hostQualificationDomain)
    {
        if (hostQualificationDomain == null || hostQualificationDomain.isBlank() || user.indexOf('.') >= 0) {
            return Optional.empty();
        }
        return requestHost(request).flatMap(host -> {
            String suffix = "." + hostQualificationDomain;
            if (!host.endsWith(suffix)) {
                return Optional.empty();
            }
            String label = host.substring(0, host.length() - suffix.length());
            if (label.isEmpty() || label.indexOf('.') >= 0) {
                return Optional.empty();
            }
            return Optional.of(label + "." + user);
        });
    }

    /**
     * The externally routed host of this request.
     * <p>
     * Client-supplied {@code Forwarded} and {@code X-Forwarded-*} headers are never consulted: the
     * coordinator may honour them, so trusting them here would let a caller present one host to the
     * restriction and a different one to the authenticator. The pooled path strips those headers
     * before forwarding, and the host actually routed by the trusted proxy path is preserved rather
     * than replaced by an internal Service name.
     */
    public static Optional<String> requestHost(HttpServletRequest request)
    {
        String host = singleHeader(request, "Host").orElseGet(request::getServerName);
        if (host == null || host.isBlank()) {
            return Optional.empty();
        }
        String stripped = host;
        if (stripped.startsWith("[")) {
            int end = stripped.indexOf(']');
            return end < 0 ? Optional.empty() : Optional.of(stripped.substring(0, end + 1).toLowerCase(java.util.Locale.ROOT));
        }
        int colon = stripped.indexOf(':');
        if (colon >= 0) {
            stripped = stripped.substring(0, colon);
        }
        return stripped.isBlank() ? Optional.empty() : Optional.of(stripped.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Headers a caller must not be able to use to change the host the coordinator qualifies with.
     */
    public static boolean isForwardedHeader(String name)
    {
        return name != null && (name.equalsIgnoreCase("Forwarded") || name.regionMatches(true, 0, "X-Forwarded-", 0, 12));
    }

    private static Optional<String> basicUser(HttpServletRequest request)
    {
        Optional<String> authorization = singleHeader(request, "Authorization");
        if (authorization.isEmpty()) {
            return Optional.empty();
        }
        String value = authorization.orElseThrow();
        if (!value.regionMatches(true, 0, "Basic ", 0, 6)) {
            // A Bearer token or cookie claim is unverified at this layer and must never key the gate.
            return Optional.empty();
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(value.substring(6).trim());
        }
        catch (IllegalArgumentException e) {
            throw error(401, "Malformed Basic authorization");
        }
        String credential = new String(decoded, StandardCharsets.ISO_8859_1);
        List<String> parts = List.of(credential.split(":", 2));
        if (parts.size() != 2 || parts.getFirst().isEmpty()) {
            throw error(401, "Malformed Basic authorization");
        }
        return Optional.of(parts.getFirst());
    }
}
