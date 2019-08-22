/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.entities;

/**
 * Represent a Payload to be sent thru databus
 *
 * @param <P> the data type of the payload. Thus, SDK's user can its own implmented payload
 *
 * <p>
 * <b><i>Example: How to send a user's defined message</i></b>
 * </p>
 * <pre>
 * {@code
 *    Producer<MyMessage> producer = new DatabusProducer<>(config,new MyMessageSerializer());
 *    MessagePayload<MyMessage> payload = new MessagePayload<>(new MyMessage("Hello World"));
 *    RoutingData routingData = new RoutingData("topic1");
 *    ProducerRecord<MyMessage> record = new ProducerRecord<>(routingData,null,payload);
 *    producer.send(record, new ProducerCallback());
 *
 *    public class ProducerCallback implements Callback {
 *       public void onCompletion(RecordMetadata metadata, Exception exception) {
 *       if(exception != null) {
 *           e.printStackTrace(); // There was an error when send
 *       }
 *       System.out.println(
 *           "MSG SENT  TOPICS:"+ metadata.topic() + " PARTITION:" + metadata.partition()
 *           + " OFFSET:" + metadata.offset() );
 *       }
 *    }
 *
 *    public final class MyMessage {
 *      private final String message;
 *      public MyMessage(final String message){
 *        this.message = message;
 *      }
 *      public String getMessage() {
 *        return message;
 *      }
 *      public String toString() {
 *        return getMessage();
 *      }
 *    }
 *
 *    public class MyMessageSerializer implements Serializer<MyMessage> {
 *      public byte[] serialize(MyMessage message) {
 *        return message.getMessage().getBytes();
 *      }
 *    }
 * }
 * </pre>
 *
 * <p>
 * <b><i>Example: How to read a user's defined message</i></b>
 * </p>
 * <pre>
 * {@code
 *   Properties consumerProps = new Properties();
 *   consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *   consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
 *   consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
 *   consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
 *   consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
 *   consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
 *
 *   Consumer<MyMessage> consumer = new DatabusConsumer<>(consumerProps,new MyMessageDeserializer());
 *   consumer.subscribe(Collections.singletonList("topic1"));
 *   ConsumerRecords<MyMessage> records = consumer.poll(POLL_TIME);
 *
 *   for (ConsumerRecord<MyMessage> record : records) {
 *      StringBuilder headers = new StringBuilder().append("[");
 *      record.getHeaders().getAll().forEach( (k,v) -> headers.append("[" + k +":"+ v +"]"));
 *      headers.append("]");
 *
 *      System.out.println("MSG RECV <-- TOPICS:" + record.getComposedTopic()
 *      + " KEY:" + record.getKey()
 *      + " PARTITION:" + record.getPartition()
 *      + " OFFSET:" + record.getOffset()
 *      + " HEADERS:" + headers
 *      + " PAYLOAD:" + record.getMessagePayload().getMessagePayload());
 *   }
 *
 *   public class MyMessageDeserializer implements Deserializer<MyMessage> {
 *      public MyMessage deserialize(byte[] data) {
 *        return new MyMessage(new String(data)) ;
 *      }
 *   }
 *
 * }
 * </pre>
 */
public final class MessagePayload<P> {

    private final P payload;

    /**
     * @param payload payload
     */
    public MessagePayload(final P payload) {
        this.payload = payload;
    }

    /**
     * @return a payload
     */
    public P getPayload() {
        return payload;
    }
}
