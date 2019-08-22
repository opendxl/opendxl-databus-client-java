/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.credential;

import java.util.HashMap;
import java.util.Map;

/**
 * SSL Credential.
 * <p>
 * It keeps a set of settings related to TLS connection and TLS authentication/authorization.
 * It is used by Producer and Consumers to establish a TLS connection with a Kafka broker
 *
 */
public final class SSLCredential implements Credential {

    private static final String SECURITY_PROTOCOL_KEY = "security.protocol";
    private static final String SSL_TRUSTSTORE_LOCATION_KEY = "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD_KEY = "ssl.truststore.password";
    private static final String SSL_KEYSTORE_LOCATION_KEY = "ssl.keystore.location";
    private static final String SSL_KEYSTORE_PASSWORD_KEY = "ssl.keystore.password";
    private static final String SSL_KEY_PASSWORD_KEY = "ssl.key.password";

    private final Map<String, Object> config;

    public SSLCredential(final String trustStoreFileName,
                         final String trustStorePassword,
                         final String keyStoreFileName,
                         final String keyStorePassword,
                         final String keyPassword) {

        config = new HashMap<>();
        config.put(SECURITY_PROTOCOL_KEY, "SSL");
        config.put(SSL_TRUSTSTORE_LOCATION_KEY, trustStoreFileName);
        config.put(SSL_TRUSTSTORE_PASSWORD_KEY, trustStorePassword);
        config.put(SSL_KEYSTORE_LOCATION_KEY, keyStoreFileName);
        config.put(SSL_KEYSTORE_PASSWORD_KEY, keyStorePassword);
        config.put(SSL_KEY_PASSWORD_KEY, keyPassword);
    }

    public Map<String, Object> getCredentialConfig() {
        return config;
    }

}
