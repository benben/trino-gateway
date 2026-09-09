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
import jakarta.ws.rs.core.Response;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class TransactionIdentity
{
    private final byte[] key;

    public TransactionIdentity(String key)
    {
        this.key = key.getBytes(UTF_8);
    }

    public String owner(HttpServletRequest request)
    {
        String authorization = singleHeader(request, "Authorization")
                .orElseThrow(() -> error(401, "Transaction-aware statement requests require Basic authorization"));
        if (authorization.length() > 8192 || !authorization.regionMatches(true, 0, "Basic ", 0, 6)) {
            throw error(401, "Transaction-aware routing currently supports stable Basic credentials only");
        }
        byte[] credential;
        String decoded;
        try {
            credential = Base64.getDecoder().decode(authorization.substring(6));
            decoded = UTF_8.newDecoder().decode(ByteBuffer.wrap(credential)).toString();
        }
        catch (IllegalArgumentException | CharacterCodingException e) {
            throw error(401, "Malformed Basic authorization");
        }
        int separator = decoded.indexOf(':');
        if (separator < 1 || decoded.chars().anyMatch(character -> character < 32 || character == 127)) {
            throw error(401, "Malformed Basic authorization");
        }
        String user = singleHeader(request, "X-Trino-User").orElse(decoded.substring(0, separator));
        String originalUser = singleHeader(request, "X-Trino-Original-User").orElse(user);
        if (user.isBlank() || originalUser.isBlank() || user.length() > 1024 || originalUser.length() > 1024) {
            throw error(400, "Invalid Trino user context");
        }
        return digest("credential\0" + Base64.getEncoder().encodeToString(credential)) + ":" + digest("context\0" + user + "\0" + originalUser);
    }

    public void validateContinuation(HttpServletRequest request, String recordedOwner)
    {
        boolean hasUser = singleHeader(request, "X-Trino-User").isPresent() || singleHeader(request, "X-Trino-Original-User").isPresent();
        if (singleHeader(request, "Authorization").isEmpty()) {
            if (hasUser) {
                throw error(401, "A continuation with user context must include its credentials");
            }
            return;
        }
        String suppliedOwner = owner(request);
        String expected = hasUser ? recordedOwner : recordedOwner.substring(0, 64);
        String supplied = hasUser ? suppliedOwner : suppliedOwner.substring(0, 64);
        if (!MessageDigest.isEqual(expected.getBytes(UTF_8), supplied.getBytes(UTF_8))) {
            throw error(403, "Query ownership does not match the supplied credentials");
        }
    }

    public static Optional<String> transactionId(HttpServletRequest request)
    {
        return singleHeader(request, "X-Trino-Transaction-Id")
                .filter(value -> !value.equals("NONE"))
                .map(TransactionIdentity::canonicalTransactionId);
    }

    public static String canonicalTransactionId(String value)
    {
        try {
            String canonical = UUID.fromString(value).toString();
            if (!canonical.equalsIgnoreCase(value)) {
                throw error(400, "Malformed transaction identifier");
            }
            return canonical;
        }
        catch (IllegalArgumentException e) {
            throw error(400, "Malformed transaction identifier");
        }
    }

    public static Optional<String> singleHeader(HttpServletRequest request, String name)
    {
        List<String> values = request.getHeaders(name) == null ? List.of() : Collections.list(request.getHeaders(name));
        if (values.size() > 1 || (values.size() == 1 && values.getFirst().isEmpty())) {
            throw error(400, "Ambiguous or empty " + name + " header");
        }
        return values.stream().findFirst();
    }

    private String digest(String value)
    {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(value.getBytes(UTF_8)));
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot initialize transaction identity binding", e);
        }
    }

    public static WebApplicationException error(int status, String message)
    {
        return new WebApplicationException(Response.status(status).type("text/plain").entity(message).build());
    }
}
