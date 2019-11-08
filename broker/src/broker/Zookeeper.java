/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package broker;


import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Zookeeper {
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final String ZOOKEEPER_SNAPSHOT_PREFIX_FILE_NAME = "zookeeper-snapshot";
    private static final String ZOOKEEPER_LOGS_PREFIX_FILE_NAME = "zookeeper-logs";
    private static final int ZOOKEEPER_MAX_CONNECTIONS = 16;
    private static final int ZOOKEEPER_TICK_TIME = 500;

    private int port;
    private int maxConnections;

    private ServerCnxnFactory zookeeperConnection;

    private static final Logger LOG = LoggerFactory.getLogger(Zookeeper.class);


    public Zookeeper(int port) {
        this.port = port;
        this.maxConnections = ZOOKEEPER_MAX_CONNECTIONS;
    }

    public Zookeeper(int port, int maxConnections) {
        this.port = port;
        this.maxConnections = maxConnections;
    }

    public void startup() {

        final File snapshotDir;
        final File logDir;
        try {
            snapshotDir = java.nio.file.Files.createTempDirectory(ZOOKEEPER_SNAPSHOT_PREFIX_FILE_NAME).toFile();
            logDir = java.nio.file.Files.createTempDirectory(ZOOKEEPER_LOGS_PREFIX_FILE_NAME).toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    FileUtils.deleteDirectory(snapshotDir);
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                }
            }

        });

        try {
            int tickTime = ZOOKEEPER_TICK_TIME;
            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            this.zookeeperConnection = NIOServerCnxnFactory.createFactory();
            this.zookeeperConnection.configure(new InetSocketAddress(ZOOKEEPER_HOST, port), maxConnections);
            zookeeperConnection.startup(zkServer);
            LOG.info("Zookeeper node started: " + "localhost:" + port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start ZooKeeper", e);
        }
    }

    public synchronized void shutdown() {
        if (zookeeperConnection != null) {
            zookeeperConnection.shutdown();
            zookeeperConnection = null;
        }
        LOG.info("Zookeeper has stopped");
    }
}
