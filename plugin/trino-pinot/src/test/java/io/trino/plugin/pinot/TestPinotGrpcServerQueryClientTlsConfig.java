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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientTlsConfig;
import org.testng.annotations.Test;

import java.util.Map;

public class TestPinotGrpcServerQueryClientTlsConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotGrpcServerQueryClientTlsConfig.class)
                        .setKeyStoreType("jks")
                        .setKeyStorePath(null)
                        .setKeyStorePassword(null)
                        .setTrustStoreType("jks")
                        .setTrustStorePath(null)
                        .setTrustStorePassword(null)
                        .setSslProvider("JDK"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.grpc.tls.key-store-type", "other")
                .put("pinot.grpc.tls.key-store-path", "/root")
                .put("pinot.grpc.tls.key-store-password", "password")
                .put("pinot.grpc.tls.trust-store-type", "other")
                .put("pinot.grpc.tls.trust-store-path", "/root")
                .put("pinot.grpc.tls.trust-store-password", "password")
                .put("pinot.grpc.tls.ssl-provider", "OPENSSL")
                .buildOrThrow();
        PinotGrpcServerQueryClientTlsConfig expected = new PinotGrpcServerQueryClientTlsConfig()
                .setKeyStoreType("other")
                .setKeyStorePath("/root")
                .setKeyStorePassword("password")
                .setTrustStoreType("other")
                .setTrustStorePath("/root")
                .setTrustStorePassword("password")
                .setSslProvider("OPENSSL");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
