/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.credential;

import java.util.HashMap;
import java.util.Map;

/**
 * SSL SASL PLAIN Credential.
 * <p>
 * It holds a set of settings related to a TLS connection and SASL-SCRAM-512 authentication/authorization.
 * It is used by Producer and Consumers to establish a TLS connection with a Kafka broker.
 *
 */
public final class SSLSaslScramSHA512Credential implements Credential {

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
     * The sasl mechanism key.
     */
    private static final String SASL_MECHANISM_KEY = "sasl.mechanism";

    /**
     * The sasl jaas config key.
     */
    private static final String SASL_JAAS_CONFIG_KEY = "sasl.jaas.config";

    /**
     * The config map.
     */
    private final Map<String, Object> config;

    /**
     * Constructor
     *
     * @param trustStoreFileName The trust-store public key file name.
     * @param trustStorePassword The trust-store public key password.
     * @param userName The user name.
     * @param userPassword The user password.
     */
    public SSLSaslScramSHA512Credential(final String trustStoreFileName,
                                        final String trustStorePassword,
                                        final String userName,
                                        final String userPassword) {

        final String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"" + userName + "\""
                + "password=\"" + userPassword + "\";";

        config = new HashMap<>();
        config.put(SECURITY_PROTOCOL_KEY, "SASL_SSL");
        config.put(SSL_TRUSTSTORE_LOCATION_KEY, trustStoreFileName);
        config.put(SSL_TRUSTSTORE_PASSWORD_KEY, trustStorePassword);
        config.put(SASL_MECHANISM_KEY, "SCRAM-SHA-512");
        config.put(SASL_JAAS_CONFIG_KEY, saslJaasConfig);
    }

    /**
     * Gets config map.
     * @return The config map.
     */
    public Map<String, Object> getCredentialConfig() {
        return config;
    }

}
