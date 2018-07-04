package net.heartsavior.flink.utils

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{datastream, environment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class TruckGeoSource(bootstrapServers: String, topic: String) extends StreamTableSource[Row]
  with DefinedRowtimeAttributes {
  val names = Array[String]("eventTime", "eventSource", "truckId", "driverId",
    "driverName", "routeId", "route", "eventType",
    "latitude", "longitude", "correlationId", "eventTimestamp")

  val types = Array[TypeInformation[_]](
    Types.STRING, Types.STRING, Types.INT, Types.INT,
    Types.STRING, Types.INT, Types.STRING, Types.STRING,
    Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.SQL_TIMESTAMP)

  val returnType = Types.ROW(names, types)

  override def getDataStream(execEnv: environment.StreamExecutionEnvironment): datastream.DataStream[Row] = {
    val sourceProp = new Properties()
    sourceProp.setProperty("bootstrap.servers", bootstrapServers)
    sourceProp.setProperty("group.id", "geo-group")
    sourceProp.setProperty("auto.offset.reset", "latest")

    val consumer = new FlinkKafkaConsumer011[ObjectNode](
      topic,
      new JSONKeyValueDeserializationSchema(false),
      sourceProp)

    execEnv.addSource(consumer)
      .map(new MapFunction[ObjectNode, TruckGeoEvent] {
        override def map(value: ObjectNode): TruckGeoEvent = TruckGeoEvent.fromJson(value)
      })
      .map(new MapFunction[TruckGeoEvent, Row] {
        override def map(event: TruckGeoEvent): Row = Row.of(
          event.eventTime, event.eventSource, event.truckId, event.driverId,
          event.driverName, event.routeId, event.route, event.eventType,
          event.latitude, event.longitude, event.correlationId, event.eventTimestamp)
      })
      // NOTE: parameter of returns() must be the same object (same identity)
      // as return value of getReturnType
      .returns(getReturnType)
  }

  override def getRowtimeAttributeDescriptors: java.util.List[RowtimeAttributeDescriptor] = {
    List(new RowtimeAttributeDescriptor("eventTimestamp", new ExistingField("eventTimestamp"),
      new BoundedOutOfOrderTimestamps(Time.minutes(1).toMilliseconds))).asJava
  }

  override def getReturnType: TypeInformation[Row] = {
    returnType
  }

  override def getTableSchema: TableSchema = {
    new TableSchema(names, types)
  }
}
