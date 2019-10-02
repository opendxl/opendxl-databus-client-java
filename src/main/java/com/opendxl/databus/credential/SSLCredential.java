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
 * It is used by Producer and Consumers to establish a TLS connection with a Kafka broker.
 *
 */
public final class SSLCredential implements Credential {

    /**
     * The security protocol key.
     */
    private static final String SECURITY_PROTOCOL_KEY = "security.protocol";

    /**
     * The ssl truststore location key.
     */
    private static final String SSL_TRUSTSTORE_LOCATION_KEY = "ssl.truststore.location";

    /**
     * The ssl truststore password key.
     */
    private static final String SSL_TRUSTSTORE_PASSWORD_KEY = "ssl.truststore.password";

    /**
     * The ssl keystore location key.
     */
    private static final String SSL_KEYSTORE_LOCATION_KEY = "ssl.keystore.location";

    /**
     * The ssl keystore password key.
     */
    private static final String SSL_KEYSTORE_PASSWORD_KEY = "ssl.keystore.password";

    /**
     * The ssl key password key.
     */
    private static final String SSL_KEY_PASSWORD_KEY = "ssl.key.password";

    /**
     * The config map.
     */
    private final Map<String, Object> config;

    /**
     * Constructor
     *
     * @param trustStoreFileName The trust-store public key file name.
     * @param trustStorePassword The trust-store public key password.
     * @param keyStoreFileName The key-store private key password.
     * @param keyStorePassword The key-store private key password.
     * @param keyPassword The password.
     */
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

    /**
     * Gets config map.
     * @return The config map.
     */
    public Map<String, Object> getCredentialConfig() {
        return config;
    }

}
