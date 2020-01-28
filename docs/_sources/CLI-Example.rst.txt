CLI (Command Line Interface)
----------------------------

The OpenDXL Databus client can be invoked from command line interface (CLI)
for testing or checking purposes. Executing the CLI like a standard Java
library with no arguments displays help information:

::

    $ java -jar dxldatabusclient-2.4.0.jar

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
    --consume-records <Integer: consume-   Number of expected records to finish
      records>                               the command line.  (default: 1)
    --consume-timeout <Integer: consume-   Max time the command line waits for
      timeout>                               finishing a consumer operation.
                                             Optional parameter, if absent, it
                                             defaults to 15000 ms. (default: 15000)
    --from-topic <String: from-topic>      Comma-separated topic name list to
                                             consume. Example: topic1,topic2,..., 
                                             topicN                               
    --headers [String: headers]            The producer headers:  (default: )     
    --msg <String: message>                message to be produced                 
    --operation <String: operation>        Operations: produce | consume
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

In order to get records from the Databus client, the user has to invoke a
few CLI operations. Operations arguments are placed after
``--operation`` option. For instance:

::

    $ java -jar dxldatabusclient-2.4.0.jar --operation <OPERATION_ARGUMENT> ...

Operation Arguments
^^^^^^^^^^^^^^^^^^^

+-----------------------+-----------------------------------------+
| Operation Arguments   | Description                             |
+=======================+=========================================+
| ``--produce``         | produce a message to a Kafka topic.     |
+-----------------------+-----------------------------------------+
| ``--consume``         | consume a message from a Kafka topic.   |
+-----------------------+-----------------------------------------+

The invocation of CLI operations to get records is: ``produce`` -> ``consume`` .

A message must be produced to a topic prior to consuming it.

produce
^^^^^^^

An operation which produces a message to a specific broker with an
associated topic. Optional arguments can be added to attach partition,
sharding key, tenant group and headers.

+-----------------------+----------------------------------------------------+
| Mandatory Arguments   | Description                                        |
| for produce           |                                                    |
+=======================+====================================================+
| ``--operation``       | The operation name. In this case, the              |
|                       | operation name is ``produce``                      |
+-----------------------+----------------------------------------------------+
| ``--to-topic``        | The topic name for the message.                    |
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
|                     | >= 0.                       | assigns    |
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

    $ java -jar dxldatabusclient-2.4.0.jar \
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

An operation which receives messages from specified topics at
specified brokers.

+----------------------+----------------+
| Mandatory Arguments  | Description    |
| for consume          |                |
+======================+================+
| ``--operation``      | The operation  |
|                      | name. In this  |
|                      | case,          |
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
|                       | a member of.                | empty string  |
+-----------------------+-----------------------------+---------------+
| ``--config``          | The consumer configuration  | Blank with    |
|                       | in comma separated          | empty string  |
|                       | property-value pairs.       |               |
|                       | For example:                |               |
|                       | enable.auto.commit=false,   |               |
|                       | request.timeout.ms=61000,   |               |
|                       | session.timeout.ms=60000,   |               |
|                       | auto.offset.reset=earliest, |               |
|                       | auto.commit.interval.ms=0   |               |
+-----------------------+-----------------------------+---------------+
| ``--consume-records`` | Number of expected records  | 1 record      |
|                       | to finish command line.     |               |
|                       | CLI polls for new records   |               |
|                       | until one of the following  |               |
|                       | occurs: timeout has         |               |
|                       | elapsed or number of records|               |
|                       | received were greater than  |               |
|                       | this value.                 |               |
+-----------------------+-----------------------------+---------------+
| ``--consume-timeout`` | Maximum time the command    | 15000         |
|                       | line waits for finishing    | milliseconds  |
|                       | a consume operation.        |               |
|                       | CLI polls for new records   |               |
|                       | until one of the following  |               |
|                       | occurs: timeout has elapsed |               |
|                       | or number of received       |               |
|                       | records were greater than   |               |
|                       | ``consume-records`` value.  |               |
+-----------------------+-----------------------------+---------------+

example
'''''''

::

    java -jar dxldatabusclient-2.4.0.jar \
    --operation consume \
    --from-topic <TOPIC_1,TOPIC_2,...,TOPIC_N> \
    --brokers <BROKER_1_IP:BROKER_1_PORT,BROKER_2_PORT:BROKER_2_PORT,...> \
    --consume-timeout <CONSUME-TIMEOUT-TO-FINISH-CLI> \
    --consume-records <CONSUME-RECORDS-NUMBER-TO-FINISH-CLI> \
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
