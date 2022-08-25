/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.producer.internal.ProducerDefaultConfiguration;
import com.opendxl.databus.serialization.internal.MessageSerializer;
import com.opendxl.databus.common.internal.adapter.DatabusProducerRecordAdapter;
import com.opendxl.databus.credential.Credential;
import com.opendxl.databus.serialization.Serializer;
import com.opendxl.databus.serialization.internal.DatabusKeySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Publishes records to Databus.
 * <p>
 * The producer is <i>thread safe</i>. Sharing a single producer instance across threads will
 * generally be faster than having multiple instances.
 * <p>
 * Here is a simple example of using the DatabusProducer to send records.
 * <pre>
 * {@code
 * // Prepare DatabusProducer configuration
 * Map config = new HashMap<String,Object>();
 * config.put("bootstrap.servers", "localhost:9092");
 * config.put("client.id", "DemoProducer");
 *
 * // Create a }{@link RoutingData#RoutingData(String, String, String)}
 * {@code getInstance
 * String topic = "topic1";
 * String shardingKey = "messageKey"; // optional
 * String tenantGroup = group0        // optional
 * RoutingData routeInformation = new RoutingData(topic, shardingKey, tenantGroup);
 *
 * // Create a }{@link Headers}{@code getInstance
 * Headers headers = new Headers();
 * headers.put(HeadersField.SOURCE_ID, "23452145-23452435-3245432");
 * headers.put(HeadersField.TENANT_ID, "578-790-870-363265");
 *
 * // Create a }{@link MessagePayload}{@code getInstance
 * String message = "Hello World";
 * MessagePayload<byte[]> payload = new MessagePayload<>(message.getBytes());
 *
 * // Create a }{@link DatabusProducer}{@code getInstance
 * Producer<byte[]> producer = new DatabusProducer<>(config, new ByteArraySerializer());
 *
 * // Create a }{@link ProducerRecord}{@code getInstance
 * ProducerRecord<byte[]> record = new ProducerRecord<>(routeInformation, headers,payload)
 *
 * try {
 *    RecordMetadata result = producer.send(record, new ProducerCallback());
 * } catch (Exception e) {
 *    e.printStackTrace();
 * }
 *}
 *
 * // Implements a {@link Callback}{@code class
 * public class ProducerCallback implements Callback {
 *
 *    // A callback method the user can implement to provide asynchronous handling of request completion.
 *    This method will
 *    // be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
 *    // non-null.
 *    // @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
 *    //           occurred.
 *    // @param exception The exception thrown during processing of this record. Null if no error occurred.
 *    public void onCompletion(RecordMetadata metadata, Exception exception) {
 *       System.out.println(
 *           "MSG SENT  TOPICS:"+ metadata.topic() + " PARTITION:" + metadata.partition() +
 *           " OFFSET:" + metadata.offset() );
 *      }
 * }
 * }
 * </pre>
 */
public class DatabusProducer<P> extends Producer<P> {

    /**
     * A DatabusProducer is instantiated by providing a set of key-value as configuration.
     * Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#newproducerconfigs">here</a>.
     * Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>Specific key-value from {@link ProducerDefaultConfiguration} will be added unless they are provided by
     * SDK user</p>

     * @param configs The DatabusProducer configs
     * @param messageSerializer an instance of {@link MessageSerializer} provided by SDK's user, used to transform
     *                          the message to byte[]
     *                          when {@link Producer#send(ProducerRecord, Callback)} sends the message
     * @throws DatabusClientRuntimeException if a DatabusProducer instance was not able to be created
     */
    public DatabusProducer(final Map<String, Object> configs, final Serializer<P> messageSerializer) {
        this(configs, messageSerializer, null);
    }

    /**
     * A DatabusProducer is instantiated by providing a set of key-value as configuration.
     * Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#newproducerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>Specific key-value from {@link ProducerDefaultConfiguration} will be added unless they are provided by
     * SDK user</p>

     * @param configs The DatabusProducer configs
     * @param messageSerializer an instance of {@link MessageSerializer} provided by SDK's user, used to
     *                          transform the message to byte[]
     *                          when {@link Producer#send(ProducerRecord, Callback)} sends the message
     * @param credential identity to authentication/authorization
     *
     * @throws DatabusClientRuntimeException if a DatabusProducer instance was not able to be created
     */
    public DatabusProducer(final Map<String, Object> configs, final Serializer<P> messageSerializer,
                           final Credential credential) {
        try {
            this.produceKafkaHeaders = true;
            setFieldMembers(messageSerializer);
            this.setConfiguration(overrideConfig(configs));
            this.configureCredential(getConfiguration(), credential);
            setProducer(new KafkaProducer(this.getConfiguration(), getKeySerializer(), getValueSerializer()));
            setClientId((String) configs.get(ProducerConfig.CLIENT_ID_CONFIG));
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabusClientRuntimeException(DATABUS_PRODUCER_INSTANCE_CANNOT_BE_CREATED_MESSAGE
                    + e.getMessage(), e, DatabusProducer.class);
        }
    }

    /**
     * A producer is instantiated by providing a set of key-value as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#newproducerconfigs">here</a>.
     * <p>Specific key-value from {@link ProducerDefaultConfiguration} will be added unless they are provided by
     * SDK user</p>
     *
     * @param properties The DatabusProducer configs
     * @param messageSerializer an instance of {@link MessageSerializer} provided by SDK's user, used to transform
     *                          the message to byte[]
     *                          when {@link Producer#send(ProducerRecord, Callback)} sends the message

     * @throws DatabusClientRuntimeException if a DatabusProducer instance was not able to be created
     */
    public DatabusProducer(final Properties properties, final Serializer<P> messageSerializer) {
        this(properties, messageSerializer, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#newproducerconfigs">here</a>.
     * <p>Specific key-value from {@link ProducerDefaultConfiguration} will be added unless they are provided by SDK
     * user</p>
     *
     * @param properties The DatabusProducer configs
     * @param messageSerializer a instance of {@link MessageSerializer} provided by SDK's user, used to transform
     *                          the message to byte[]
     *                          when {@link Producer#send(ProducerRecord, Callback)} sends the message
     * @param credential identity to authenticate/authorization
     *
     * @throws DatabusClientRuntimeException if a DatabusProducer instance was not able to be created
     */
    public DatabusProducer(final Properties properties, final Serializer<P> messageSerializer,
                           final Credential credential) {
        try {
            this.produceKafkaHeaders = true;
            setFieldMembers(messageSerializer);
            Properties fixedProperties = overrideConfig(properties);
            this.setConfiguration((Map) fixedProperties);
            this.configureCredential(getConfiguration(), credential);
            setProducer(new KafkaProducer(this.getConfiguration(), getKeySerializer(), getValueSerializer()));
            setClientId((String) fixedProperties.get(ProducerConfig.CLIENT_ID_CONFIG));
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabusClientRuntimeException(DATABUS_PRODUCER_INSTANCE_CANNOT_BE_CREATED_MESSAGE
                    + e.getMessage(), e, DatabusProducer.class);
        }
    }

    /**
     * Assing a {@link Serializer} to a DatabusProducer instance
     *
     * @param messageSerializer It is a serializer which will be assigned to the DatabusProducer
     */
    private void setFieldMembers(final Serializer<P> messageSerializer) {
        if (messageSerializer == null) {
            throw new DatabusClientRuntimeException(DATABUS_PRODUCER_INSTANCE_CANNOT_BE_CREATED_MESSAGE
                    + "Message Serializer cannot be null" , DatabusProducer.class);
        }

        setKeySerializer(new DatabusKeySerializer());
        setValueSerializer(new MessageSerializer());
        setDatabusProducerRecordAdapter(new DatabusProducerRecordAdapter<P>(messageSerializer));
    }

    /**
     * This method add settings kept into {@link ProducerDefaultConfiguration} unless they are in configs parameter
     *
     * @param configs It is the configuration that a SDK's user sends when a new instance of DatabusProducer
     *                is created
     * @return The configuration overrode
     */
    private Map<String, Object> overrideConfig(final Map<String, Object> configs) {
        if (configs == null) {
            throw new DatabusClientRuntimeException(DATABUS_PRODUCER_INSTANCE_CANNOT_BE_CREATED_MESSAGE
                    + "config properties cannot be null" , DatabusProducer.class);
        }
        Map<String, Object> overrodeConfiguration = new HashMap(configs);
        for (Object defaultPropertyKey : ProducerDefaultConfiguration.getAll().keySet()) {
            if (!overrodeConfiguration.containsKey(defaultPropertyKey)) {

                overrodeConfiguration.put((String) defaultPropertyKey,
                        ProducerDefaultConfiguration.get((String) defaultPropertyKey));
            }
        }

        String clientId = (String) overrodeConfiguration.get(ProducerConfig.CLIENT_ID_CONFIG);
        if (clientId != null && !clientId.trim().isEmpty()) {
            overrodeConfiguration.put(ProducerConfig.CLIENT_ID_CONFIG, clientId.trim());
        } else {
            clientId = UUID.randomUUID().toString();
            overrodeConfiguration.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }

        return overrodeConfiguration;
    }

    /**
     * This method add settings kept into {@link ProducerDefaultConfiguration} unless they are in configs parameter
     *
     * @param properties It is the configuration that a SDK's user sends when a new instance of DatabusProducer
     *                   is created
     * @return The configuration overrode
     */
    private Properties overrideConfig(final Properties properties) {
        if (properties == null) {
            throw new DatabusClientRuntimeException(DATABUS_PRODUCER_INSTANCE_CANNOT_BE_CREATED_MESSAGE
                    + "config properties cannot be null" , DatabusProducer.class);
        }
        Properties overrodeConfiguration = properties;

        for (Object defaultPropertyKey : ProducerDefaultConfiguration.getAll().keySet()) {
            if (!overrodeConfiguration.containsKey(defaultPropertyKey)) {

                overrodeConfiguration.put((String) defaultPropertyKey,
                        ProducerDefaultConfiguration.get((String) defaultPropertyKey));
            }
        }

        String clientId = (String) overrodeConfiguration.get(ProducerConfig.CLIENT_ID_CONFIG);
        if (clientId != null && !clientId.trim().isEmpty()) {
            overrodeConfiguration.put(ProducerConfig.CLIENT_ID_CONFIG, clientId.trim());
        } else {
            clientId = UUID.randomUUID().toString();
            overrodeConfiguration.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }

        return overrodeConfiguration;
    }

    /**
     * This method add credential kept into a configuration for the {@link Producer} instance
     *
     * @param configuration It is the configuration that a SDK's user sends when a new instance of DatabusProducer
     *                      is created
     * @param credential Identity to authentication/authorization
     */
    private void configureCredential(final Map<String, Object> configuration, final Credential credential) {
        if (credential == null) {
            return;
        }

        final Map<String, Object> credentialConfig = credential.getCredentialConfig();

        for (Object key : credentialConfig.keySet()) {
            configuration.put((String) key, credentialConfig.get(key));
        }
    }

    /**
     * String to prepend to error messages related to creation of DatabusProducer instances
     */
    private static final String DATABUS_PRODUCER_INSTANCE_CANNOT_BE_CREATED_MESSAGE =
            "A DatabusProducer instance cannot be created: ";

}
