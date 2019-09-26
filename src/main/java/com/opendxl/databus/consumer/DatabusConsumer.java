/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.credential.Credential;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.serialization.Deserializer;
import com.opendxl.databus.serialization.internal.DatabusKeyDeserializer;
import com.opendxl.databus.serialization.internal.MessageDeserializer;
import com.opendxl.databus.common.internal.adapter.ConsumerRecordsAdapter;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * It consumes records listening to a set of topics that were published for a
 * {@link DatabusProducer}
 * <p>
 * Here is a simple example of using the databus consumer to read records.
 * </p>
 * <pre>
 * {@code
 * // Prepare Databus Consumer configuration
 * Properties consumerProps = new Properties();
 * consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
 * consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
 * consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
 * consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
 *
 * // Create a }{@link DatabusConsumer}{@code getInstance
 * Consumer<byte[]> consumer = new DatabusConsumer(consumerProps, new ByteArrayDeserializer());
 *
 * // The consumer subscribes to a topic list
 * consumer.subscribe(Collections.singletonList("topic1"));
 *
 * // Consumer reads a list records from Databus topics
 * ConsumerRecords<byte[]> records = consumer.poll(500L);
 *
 * // Iterate records
 * // A }{@link ConsumerRecord}{@code getInstance will be created and deserilized for each message read from databus
 * for (ConsumerRecord<byte[]> record : records) {
 *       System.out.println("MSG RECV <-- TOPICS:" + record.getComposedTopic()
 *       + " KEY:" + record.getKey()
 *       + " PARTITION:" + record.getPartition()
 *       + " OFFSET:" + record.getOffset()
 *       + " HEADERS:" + headers
 *       + " PAYLOAD:" + record.getMessagePayload().getMessagePayload());
 *
 * }
 * }
 * </pre>
 */
public class DatabusConsumer<P> extends Consumer<P> {

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     *
     * @param configs The consumer configs
     * @param messageDeserializer a {@link Deserializer} getInstance implementd by SDK's user
     * @throws DatabusClientRuntimeException if a DatabusConsumer getInstance was not able to be created
     */
    public DatabusConsumer(final Map<String, Object> configs, final Deserializer<P> messageDeserializer) {
        this(configs, messageDeserializer, null);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     *
     * @param configs The consumer configs
     * @param messageDeserializer a {@link Deserializer} getInstance implementd by SDK's user
     * @param credential identity to authenticate/authorization
     *
     * @throws DatabusClientRuntimeException if a DatabusConsumer getInstance was not able to be created
     */
    public DatabusConsumer(final Map<String, Object> configs, final Deserializer<P> messageDeserializer,
                           final Credential credential) {
        try {
            Map<String, Object> configuration = configureCredential(configs, credential);
            configuration = configureClientId(configs);
            setFieldMembers(messageDeserializer, configuration);
            setConsumer(new KafkaConsumer(configuration, getKeyDeserializer(), getValueDeserializer()));
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("A DatabusConsumer getInstance cannot be created: "
                    + e.getMessage(), e, DatabusConsumer.class);
        }
    }

    /**
     * A consumer is instantiated by providing a {@link Properties} object as configuration. Valid
     * configuration strings are documented at {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     * A consumer is instantiated by
     * providing a
     * {@link Properties} object as configuration. Valid configuration strings are documented at
     * {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     *
     * @param properties The consumer configuration properties
     * @param messageDeserializer  a {@link Deserializer} getInstance implementd by SDK's user
     *
     * @throws DatabusClientRuntimeException if a DatabusConsumer getInstance was not able to be created
     */
    public DatabusConsumer(final Properties properties, final Deserializer<P> messageDeserializer) {
        this(properties, messageDeserializer, null);
    }

    /**
     * A consumer is instantiated by providing a {@link Properties} object as configuration. Valid
     * configuration strings are documented at {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     * A consumer is instantiated by
     * providing a
     * {@link Properties} object as configuration. Valid configuration strings are documented at
     * {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     *
     * @param properties The consumer configuration properties
     * @param messageDeserializer  a {@link Deserializer} getInstance implementd by SDK's user
     * @param credential identity to authenticate/authorization
     *
     * @throws DatabusClientRuntimeException if a DatabusConsumer getInstance was not able to be created
     */
    public DatabusConsumer(final Properties properties, final Deserializer<P> messageDeserializer,
                           final Credential credential) {
        try {
            Map<String, Object> configuration = configureCredential((Map) properties, credential);
            configuration = configureClientId(configuration);
            setFieldMembers(messageDeserializer, configuration);
            setConsumer(new KafkaConsumer(configuration, getKeyDeserializer(), getValueDeserializer()));
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("A DatabusConsumer getInstance cannot be created: "
                    + e.getMessage(), e, DatabusConsumer.class);
        }
     }

    private void setFieldMembers(final Deserializer<P> messageDeserializer, final Map<String, Object> configuration) {
        if (messageDeserializer == null) {
            throw new DatabusClientRuntimeException("Message Deserializer cannot be null" , DatabusConsumer.class);
        }

        setKeyDeserializer(new DatabusKeyDeserializer());
        setValueDeserializer(new MessageDeserializer());
        setConsumerRecordsAdapter(new ConsumerRecordsAdapter<P>(messageDeserializer));
        setClientId((String) configuration.get(ConsumerConfiguration.CLIENT_ID_CONFIG));
    }

    private Map<String, Object> configureCredential(final Map<String, Object> configuration,
                                                    final Credential credential) {
        if (credential == null) {
            return configuration;
        }

        final Map<String, Object> credentialConfig = credential.getCredentialConfig();

        for (Object key : credentialConfig.keySet()) {
            configuration.put((String) key, credentialConfig.get(key));
        }
        return configuration;
    }

    private Map<String, Object> configureClientId(final Map<String, Object> configuration) {
        String clientId = (String) configuration.get(ConsumerConfiguration.CLIENT_ID_CONFIG);
        if (clientId != null && !clientId.trim().isEmpty()) {
            configuration.put(ConsumerConfiguration.CLIENT_ID_CONFIG, clientId.trim());
            return configuration;
        }
        clientId = UUID.randomUUID().toString();
        configuration.put(ConsumerConfiguration.CLIENT_ID_CONFIG, clientId);
        return configuration;
    }

}
