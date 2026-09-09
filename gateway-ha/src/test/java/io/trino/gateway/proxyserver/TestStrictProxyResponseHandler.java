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
package io.trino.gateway.proxyserver;

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.units.DataSize;
import io.trino.gateway.ha.config.ProxyResponseConfiguration;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class TestStrictProxyResponseHandler
{
    @Test
    void acceptsExactLimit()
            throws Exception
    {
        assertThat(handle("{}".getBytes(UTF_8), 2).body()).isEqualTo("{}");
    }

    @Test
    void rejectsTruncatedValidJsonPrefix()
    {
        assertThatThrownBy(() -> handle("{}trailing".getBytes(UTF_8), 2))
                .isInstanceOf(ProxyException.class)
                .hasMessageContaining("exceeds");
    }

    @Test
    void rejectsMalformedUtf8()
    {
        assertThatThrownBy(() -> handle(new byte[] {(byte) 0xC3, (byte) 0x28}, 10))
                .isInstanceOf(ProxyException.class)
                .hasMessageContaining("Failed reading");
    }

    private static ProxyResponseHandler.ProxyResponse handle(byte[] bytes, int limit)
            throws Exception
    {
        ProxyResponseConfiguration configuration = new ProxyResponseConfiguration();
        configuration.setResponseSize(DataSize.ofBytes(limit));
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getHeaders()).thenReturn(ImmutableListMultimap.of());
        when(response.getInputStream()).thenReturn(new ByteArrayInputStream(bytes));
        return new ProxyResponseHandler(configuration, true).handle(
                Request.Builder.prepareGet().setUri(URI.create("http://backend.example/v1/statement/query")).build(), response);
    }
}
