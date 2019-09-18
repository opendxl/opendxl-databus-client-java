CLI (Command Line Interface)
----------------------------

OpenDXL Databus client can be invoked from command line interface (CLI)
for testing or checking purposes. Executing CLI like a standard Java
library with no arguments, the output shows the help:

::

    $ java -jar opendxldatabusclient-java-sdk-<VERSION>.jar

    ERROR: There are not options
    Option (* = required)                  Description                            
    ---------------------                  -----------                            
    * --brokers <String: broker list>      Comma-separated broker list:  Example: 
                                             broker1:port1,broker2:port2,...,     
                                             brokerN:portN                        
    --cg <String: cg>                      The consumer group name.               
    --config [String: config]              The producer/consumer configuration    
                                             list: Example: linger.ms=1000,batch. 
                                             size=100000,compression.type=lz4     
    --consume-records <Integer: consume-   Consume Poll expected records. Number  
      records>                               of expected records.  (default: 1)   
    --consume-timeout <Integer: consume-   Consume Poll Timeout. Time in ms that  
      timeout>                               the consumer waits for new records   
                                             during a consume operation.          
                                             Optional parameter, if absent, it    
                                             defaults to 5000 ms. (default: 15000)
    --from-topic <String: from-topic>      Coma-separated topic name list to      
                                             consume. Example: topic1,topic2,..., 
                                             topicN                               
    --headers [String: headers]            The producer headers:  (default: )     
    --msg <String: message>                message to be produced                 
    --operation <String: operation>      Operations: produce | consume          
    --partition [String: partition]        The partition number:  (default: )     
    --sharding-key [String: sharding-key]  Sharding key (default: )               
    --tenant-group [String: tenant-group]  Tenant Group (default: )               
    --to-topic <String: to-topic>          Topic name to produce

*Note:* ``<String: FIELD>`` are mandatory placeholders which you should
respectively replace with the correct field name. ``[String: FIELD]``
are optional placeholders which you should respectively replace with the
correct field name.

Supported Operations
~~~~~~~~~~~~~~~~~~~~

In order to get records from Databus client, the user has to invoke a
few CLI operations. Operations arguments are placed after
``--operation`` option. For instance:

::

    $ java -jar opendxldatabusclient-java-sdk-<VERSION>.jar --operation <OPERATION_ARGUMENT> ...

Operation Arguments
^^^^^^^^^^^^^^^^^^^

+-----------------------+-----------------------------------------+
| Operation Arguments   | Description                             |
+=======================+=========================================+
| ``--produce``         | produce a message to a Kafka topic.     |
+-----------------------+-----------------------------------------+
| ``--consume``         | consume a message from a Kafka topic.   |
+-----------------------+-----------------------------------------+

| The invocation of CLI operations to get records is: ``produce`` ->
  ``consume`` .
| First of all, a message is needed to be produced to a topic and then
  start consuming it.

produce
^^^^^^^

It is an operation which produces a message to a specific broker with an
associated topic. Optional arguments can be added to attach partition,
sharding key, tenant group and headers.

+-----------------------+----------------------------------------------------+
| Mandatory Arguments   | Description                                        |
| for produce           |                                                    |
+=======================+====================================================+
| ``--operation``       | The operation name. In this case, always the       |
|                       | operation name is ``produce``                      |
+-----------------------+----------------------------------------------------+
| ``--to-topic``        | The topic name which will send the message.        |
+-----------------------+----------------------------------------------------+
| ``--msg``             | The message payload.                               |
+-----------------------+----------------------------------------------------+
| ``--brokers``         | The brokers list comma separated. Host and ports.  |
+-----------------------+----------------------------------------------------+

+---------------------+-----------------------------+------------+
| Optional Arguments  | Description                 | Default    |
| for produce         |                             | value      |
+=====================+=============================+============+
| ``--partition``     | The partition number, valid | Blank,     |
|                     | numbers for partition are   | which      |
|                     | >= 0.                       | assignes   |
|                     |                             | any kafka  |
|                     |                             | valid      |
|                     |                             | partition  |
|                     |                             | created.   |
+---------------------+-----------------------------+------------+
| ``--sharding-key``  | The key associated to the   | Blank with |
|                     | message.                    | empty      |
|                     |                             | string     |
+---------------------+-----------------------------+------------+
| ``--tenant-group``  | The associated tenant       | Blank with |
|                     | group.                      | empty      |
|                     |                             | string     |
+---------------------+-----------------------------+------------+
| ``--headers``       | The associated producer     | Blank with |
|                     | headers.                    | empty      |
|                     |                             | string     |
+---------------------+-----------------------------+------------+
| ``--config``        | The producer configuration. | Blank with |
|                     | For example:                | empty      |
|                     | linger.ms=1000,batch.size=1 | string     |
|                     | 00000,compression.type=lz4  |            |
+---------------------+-----------------------------+------------+

example
'''''''

::

    $ java -jar opendxlstreamingclient-java-sdk-<VERSION>.jar \
    --operation produce \
    --to-topic <TOPIC_NAME> \
    --brokers <0.0.0.0>:<PORT> \
    --msg <MESSAGE> \
    --tenant-group <TENANT-GROUP> \ 
    --sharding-key <KEY> \
    --partition <PARTITION-NUMBER> \

::

    {
        "code":"OK",
        "result": "SHARDING-KEY: <KEY> TOPICS:<TOPIC_NAME>-<TENANT-GROUP> PARTITION:<PARTITION-NUMBER> OFFSET:0",
        "options":{
            "[to-topic]":[<TOPIC_NAME>],
            "[brokers]":[<0.0.0.0>:<PORT> ],
            "[msg]":[<MESSAGE>],
            "[consume-timeout]":[15000],
            "[headers]":[""],
            "[cg]":[],
            "[sharding-key]":[<KEY>],
            "[from-topic]":[],
            "[tenant-group]":[<TENANT-GROUP>],
            "[partition]":[<PARTITION-NUMBER>],
            "[consume-records]":[1],
            "[operation]":["produce"],
            "[config]":[]
        }
    }

consume
^^^^^^^

It is an operation which receives messages from specified topics at
specified brokers. Optional arguments, like consume-records or
consume-timeout, are supported to refine the record list contained in
the consumer operation result.

+----------------------+----------------+
| Mandatory Arguments  | Description    |
| for consume          |                |
+======================+================+
| ``--operation``      | The operation  |
|                      | name. In this  |
|                      | case, always   |
|                      | the operation  |
|                      | name is        |
|                      | ``consume``    |
+----------------------+----------------+
| ``--brokers``        | The brokers    |
|                      | list comma     |
|                      | separated.     |
|                      | Host and       |
|                      | ports.         |
+----------------------+----------------+
| ``--from-topic``     | A comma        |
|                      | separated      |
|                      | string list    |
|                      | with the topic |
|                      | names to       |
|                      | consume from.  |
+----------------------+----------------+

+-----------------------+-----------------------------+---------------+
| Optional Arguments    | Description                 | Default value |
| for consume           |                             |               |
+=======================+=============================+===============+
| ``--tenant-group``    | The associated tenant       | Blank with    |
|                       | group.                      | empty string  |
+-----------------------+-----------------------------+---------------+
| ``--cg``              | The consumer group to be    | Blank with    |
|                       | member of.                  | empty string  |
+-----------------------+-----------------------------+---------------+
| ``--config``          | The consumer configuration  | Blank with    |
|                       | in comma separated          | empty string  |
|                       | property-value pairs.For    |               |
|                       | example:                    |               |
|                       | enable.auto.commit=false,re |               |
|                       | quest.timeout.ms=61000,sess |               |
|                       | ion.timeout.ms=60000,auto.o |               |
|                       | ffset.reset=earliest,auto.c |               |
|                       | ommit.interval.ms=0         |               |
+-----------------------+-----------------------------+---------------+
| ``--consume-records`` | Maximum number of records   | 1 record      |
|                       | to read within thespecified |               |
|                       | ``consume-timeout``. CLI    |               |
|                       | polls for new records       |               |
|                       | untilwhichever condition is |               |
|                       | first met:timeout has       |               |
|                       | elapsed, ormaximum number   |               |
|                       | of records were received.   |               |
+-----------------------+-----------------------------+---------------+
| ``--consume-timeout`` | Maximum time to wait for    | 15000         |
|                       | receiving the               | milliseconds  |
|                       | specified\ ``consume-record |               |
|                       | s``                         |               |
|                       | number. CLI polls for new   |               |
|                       | records untilwhichever      |               |
|                       | condition is first          |               |
|                       | met:timeout has elapsed     |               |
|                       | ornumber of received        |               |
|                       | records reached             |               |
|                       | ``consume-records`` value.  |               |
+-----------------------+-----------------------------+---------------+

example
'''''''

This example waits up to \\ to receive up to \\ records in total for \\
tenant group from \\ or \\ or \\ topics from \\ or \\ kafka brokers
using \\ consumer group and configuring consumer to enable auto commmit.

::

    java -jar opendxldatabusclient-java-sdk-<VERSION>.jar \
    --operation consume \
    --from-topic <TOPIC_1,TOPIC_2,...,TOPIC_N> \
    --brokers <BROKER_1_IP:BROKER_1_PORT,BROKER_2_PORT:BROKER_2_PORT,...> \
    --consume-timeout <CONSUME-TIMEOUT> \
    --consume-records <CONSUME-RECORDS-NUMBER> \
    --tenant-group <TENANT-GROUP-NAME> \
    --cg <CONSUMER-GROUP>
    --config enable.auto.commit=true 

The response example illustrates that two records are returned.

::

    {
      "code": "OK",
      "result": [
        {
          "shardingKey": "",
          "payload": "Hello Databus 002",
          "composedTopic": <TOPIC_2>-<TENANT_GROUP-NAME>,
          "topic": <TOPIC_2>,
          "tenantGroup": <TENANT-GROUP-NAME>,
          "headers": {},
          "offset": 0,
          "partition": 0,
          "timestamp": 1568303331900
        },
        {
          "shardingKey": "",
          "payload": "Hello Databus 001",
          "composedTopic": <TOPIC_1>-<TENANT-GROUP-NAME>,
          "topic": <TOPIC_1>,
          "tenantGroup": <TENANT-GROUP-NAME>,
          "headers": {},
          "offset": 0,
          "partition": 0,
          "timestamp": 1568303355250
        }
      ],
      "options": {
        "[to-topic]": [],
        "[brokers]": [
          <BROKER_1_IP:BROKER_1_PORT,BROKER_2_PORT:BROKER_2_PORT,...>
        ],
        "[msg]": [],
        "[consume-timeout]": [
          <CONSUME-TIMEOUT>
        ],
        "[headers]": [
          ""
        ],
        "[cg]": [],
        "[sharding-key]": [
          ""
        ],
        "[from-topic]": [
          <TOPIC_1,TOPIC_2,...,TOPIC_N>
        ],
        "[tenant-group]": [
          <TENANT-GROUP-NAME>
        ],
        "[partition]": [
          ""
        ],
        "[consume-records]": [
          2
        ],
        "[operation]": [
          "consume"
        ],
        "[config]": [
          "enable.auto.commit=true"
        ]
      }
    }
