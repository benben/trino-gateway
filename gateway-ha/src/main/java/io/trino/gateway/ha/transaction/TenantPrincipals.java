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
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.gateway.ha.transaction.TransactionIdentity.error;
import static io.trino.gateway.ha.transaction.TransactionIdentity.singleHeader;

/**
 * Derives the single principal a request will be authenticated as, for the deny-only pooled
 * admission restriction. The tenant that principal belongs to is looked up in the
 * controller-published mapping; it is never inferred from the shape of a name.
 * <p>
 * This is deliberately <em>not</em> authentication. The coordinator verifies the forwarded Basic
 * credential itself on every statement, and its authorization policy refuses impersonation, so a
 * claimed name can never become an authenticated principal here. The restriction only refuses to
 * dispatch work whose tenant is not admitted yet; absence of a claim grants nothing.
 * <p>
 * Parsing mirrors the coordinator byte for byte, because a restriction keyed on a different string
 * than the one the coordinator authenticates would be either bypassable or needlessly blocking:
 * <ul>
 *   <li>the credential is decoded as ISO-8859-1, not UTF-8;</li>
 *   <li>the scheme is matched as an exact case-insensitive {@code "Basic "} prefix;</li>
 *   <li>the decoded value is split on its first colon and an empty user is refused;</li>
 *   <li>a duplicate or empty identity header is refused rather than silently coalesced;</li>
 *   <li>host qualification is recomputed from the same inputs and produces <em>one</em> name. The
 *       coordinator qualifies before it authenticates and checks only the qualified name, with no
 *       fallback to the name as typed, so treating both as alternatives would let a request whose
 *       qualified principal is unmapped pass on an unrelated bare mapping.</li>
 * </ul>
 */
public final class TenantPrincipals
{
    private TenantPrincipals() {}

    private static final Pattern HOST_LABEL = Pattern.compile("[a-z0-9]([a-z0-9-]*[a-z0-9])?");

    /**
     * Returns the one name this request will be authenticated as: the credential's user qualified by
     * the routed host when qualification applies, and the user exactly as presented when it does not.
     * Empty when the request presents no Basic credential.
     * <p>
     * A request-supplied user header is deliberately never consulted — it is not evidence of
     * authentication, and user selection and impersonation stay enforced by the coordinator and its
     * policy. The returned name is only a lookup key; its tenant comes from the published mapping.
     *
     * @param hostQualificationDomains domains under which the coordinator qualifies a user with the
     *         routed host's single leading label, exactly as its password authenticator does before
     *         verifying the credential.
     * @param excludedHostLabels labels under those domains that name something operational rather
     *         than a tenant, matching the coordinator's own exclusions.
     */
    public static Optional<String> authenticatedPrincipal(HttpServletRequest request, List<String> hostQualificationDomains, Set<String> excludedHostLabels)
    {
        return basicUser(request).map(user -> qualify(user, request, hostQualificationDomains, excludedHostLabels).orElse(user));
    }

    /**
     * Recomputes the coordinator's host qualification: a routed host of exactly one permitted label
     * under a configured domain qualifies the user as {@code <label>.<user>}. Anything else leaves
     * the user unchanged, so the bare credential remains the only candidate.
     */
    public static Optional<String> qualify(String user, HttpServletRequest request, List<String> hostQualificationDomains, Set<String> excludedHostLabels)
    {
        if (hostQualificationDomains == null || hostQualificationDomains.isEmpty()) {
            return Optional.empty();
        }
        return requestHost(request).flatMap(host -> tenantLabel(host, hostQualificationDomains, excludedHostLabels))
                .map(label -> label + "." + user);
    }

    private static Optional<String> tenantLabel(String host, List<String> domains, Set<String> excludedHostLabels)
    {
        Set<String> excluded = excludedHostLabels == null
                ? Set.of()
                : excludedHostLabels.stream().map(label -> label.toLowerCase(Locale.ENGLISH)).collect(java.util.stream.Collectors.toSet());
        for (String domain : domains) {
            if (domain == null || domain.isBlank()) {
                continue;
            }
            String suffix = "." + stripTrailingDot(domain.trim().toLowerCase(Locale.ENGLISH)).replaceFirst("^\\.", "");
            if (!host.endsWith(suffix)) {
                continue;
            }
            String label = host.substring(0, host.length() - suffix.length());
            // A longer configured domain may still match, so keep looking rather than giving up here.
            if (HOST_LABEL.matcher(label).matches() && !excluded.contains(label)) {
                return Optional.of(label);
            }
        }
        return Optional.empty();
    }

    private static String stripTrailingDot(String value)
    {
        return value.endsWith(".") ? value.substring(0, value.length() - 1) : value;
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
            return end < 0 ? Optional.empty() : Optional.of(stripped.substring(0, end + 1).toLowerCase(Locale.ENGLISH));
        }
        int colon = stripped.indexOf(':');
        if (colon >= 0) {
            stripped = stripped.substring(0, colon);
        }
        // The coordinator strips a root-zone trailing dot before it qualifies, so a fully qualified
        // host must produce the same key on both sides.
        stripped = stripTrailingDot(stripped);
        return stripped.isBlank() ? Optional.empty() : Optional.of(stripped.toLowerCase(Locale.ENGLISH));
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
