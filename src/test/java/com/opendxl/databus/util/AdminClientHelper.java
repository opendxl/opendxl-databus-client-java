package com.opendxl.databus.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;

import com.opendxl.databus.producer.ProducerConfig;

public class AdminClientHelper {
    private static final String BROKER_HOST = Constants.KAFKA_HOST;
    private static final String BROKER_PORT = Constants.KAFKA_PORT;

    public static AdminClient createAdminClient() {
        final Map<String, Object> props = new HashMap<>();
        final String bootstrapServer = BROKER_HOST.concat(":").concat(BROKER_PORT);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return AdminClient.create(props);
    }
}
