package net.heartsavior.flink.datasource

import java.util.Properties

import org.apache.flink.hack.table.sources.tsextractors.IsoDateStringAwareExistingField
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps
import org.apache.flink.types.Row

object EventDataSources {

  def geoTableSource(brokers: String, topic: String): StreamTableSource[Row] = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", brokers)
    prop.setProperty("group.id", "geo-group")
    prop.setProperty("auto.offset.reset", "latest")

    Kafka010JsonTableSource.builder()
      .forTopic(topic)
      .withSchema(TableSchema.builder()
        .field("eventTime", Types.SQL_TIMESTAMP)
        .field("eventSource", Types.STRING)
        .field("truckId", Types.INT)
        .field("driverId", Types.INT)
        .field("driverName", Types.STRING)
        .field("routeId", Types.INT)
        .field("route", Types.STRING)
        .field("eventType", Types.STRING)
        .field("latitude", Types.DOUBLE)
        .field("longitude", Types.DOUBLE)
        .field("correlationId", Types.DOUBLE)
        .build())
      .forJsonSchema(TableSchema.builder()
        .field("eventTime", Types.STRING)
        .field("eventSource", Types.STRING)
        .field("truckId", Types.INT)
        .field("driverId", Types.INT)
        .field("driverName", Types.STRING)
        .field("routeId", Types.INT)
        .field("route", Types.STRING)
        .field("eventType", Types.STRING)
        .field("latitude", Types.DOUBLE)
        .field("longitude", Types.DOUBLE)
        .field("correlationId", Types.DOUBLE)
        .build())
      .withKafkaProperties(prop)
      .withRowtimeAttribute(
        "eventTime",
        new IsoDateStringAwareExistingField("eventTime"),
        new BoundedOutOfOrderTimestamps(Time.minutes(1).toMilliseconds)
      )
      .build()
  }

  def speedTableSource(brokers: String, topic: String): StreamTableSource[Row] = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", brokers)
    prop.setProperty("group.id", "speed-group")
    prop.setProperty("auto.offset.reset", "latest")

    Kafka010JsonTableSource.builder()
      .forTopic(topic)
      .withSchema(TableSchema.builder()
        .field("eventTime", Types.SQL_TIMESTAMP)
        .field("eventSource", Types.STRING)
        .field("truckId", Types.INT)
        .field("driverId", Types.INT)
        .field("driverName", Types.STRING)
        .field("routeId", Types.INT)
        .field("route", Types.STRING)
        .field("speed", Types.INT)
        .build())
      .forJsonSchema(TableSchema.builder()
        .field("eventTime", Types.STRING)
        .field("eventSource", Types.STRING)
        .field("truckId", Types.INT)
        .field("driverId", Types.INT)
        .field("driverName", Types.STRING)
        .field("routeId", Types.INT)
        .field("route", Types.STRING)
        .field("speed", Types.INT)
        .build())
      .withKafkaProperties(prop)
      .withRowtimeAttribute(
        "eventTime",
        new IsoDateStringAwareExistingField("eventTime"),
        new BoundedOutOfOrderTimestamps(Time.minutes(1).toMilliseconds)
      )
      .build()
  }

}
