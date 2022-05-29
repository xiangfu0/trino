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
package io.trino.plugin.pinot.client;

import io.airlift.configuration.Config;

public class PinotGrpcServerQueryClientTlsConfig
{
    private String keyStoreType = "jks";
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStoreType = "jks";
    private String trustStorePath;
    private String trustStorePassword;
    private String sslProvider = "JDK";

    public String getKeyStoreType()
    {
        return keyStoreType;
    }

    @Config("pinot.grpc.tls.key-store-type")
    public PinotGrpcServerQueryClientTlsConfig setKeyStoreType(String keyStoreType)
    {
        this.keyStoreType = keyStoreType;
        return this;
    }

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("pinot.grpc.tls.key-store-path")
    public PinotGrpcServerQueryClientTlsConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("pinot.grpc.tls.key-store-password")
    public PinotGrpcServerQueryClientTlsConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getTrustStoreType()
    {
        return trustStoreType;
    }

    @Config("pinot.grpc.tls.trust-store-type")
    public PinotGrpcServerQueryClientTlsConfig setTrustStoreType(String trustStoreType)
    {
        this.trustStoreType = trustStoreType;
        return this;
    }

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    @Config("pinot.grpc.tls.trust-store-path")
    public PinotGrpcServerQueryClientTlsConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    @Config("pinot.grpc.tls.trust-store-password")
    public PinotGrpcServerQueryClientTlsConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public String getSslProvider()
    {
        return sslProvider;
    }

    @Config("pinot.grpc.tls.ssl-provider")
    public PinotGrpcServerQueryClientTlsConfig setSslProvider(String sslProvider)
    {
        this.sslProvider = sslProvider;
        return this;
    }
}
