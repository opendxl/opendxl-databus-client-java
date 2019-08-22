/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.credential;

import java.util.HashMap;
import java.util.Map;

/**
 * SSL SASL PLAIN Credential.
 * <p>
 * It holds a set of settings related to a TLS connection and SASL-SCRAM-512 authentication/authorization
 * It is used by Producer and Consumers to establish a TLS connection with a Kafka broker
 *
 */
public final class SSLSaslScramSHA512Credential implements Credential {

    private static final String SECURITY_PROTOCOL_KEY = "security.protocol";
    private static final String SSL_TRUSTSTORE_LOCATION_KEY = "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD_KEY = "ssl.truststore.password";
    private static final String SASL_MECHANISM_KEY = "sasl.mechanism";
    private static final String SASL_JAAS_CONFIG_KEY = "sasl.jaas.config";

    private final Map<String, Object> config;

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

    public Map<String, Object> getCredentialConfig() {
        return config;
    }

}
