/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

/**
 * Databus Supported Message Rormats
 */
public enum MessageFormat {

   // Custom Databus message format - Headers and payload are bundled and stored in
   // Kafka message.
   // Headers are also set in Kafka message headers
   DATABUS,

   // JSON message format - Payload is stored as JSON in Kafka message. Headers are
   // set in Kafka message headers.
   JSON
}