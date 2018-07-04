package net.heartsavior.flink.datasource

import java.text.SimpleDateFormat

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

case class TruckSpeedEvent(eventTime: java.lang.String, eventSource: java.lang.String,
                           truckId: java.lang.Integer, driverId: java.lang.Integer,
                           driverName: java.lang.String, routeId: java.lang.Integer,
                           route: java.lang.String, speed: java.lang.Integer,
                           eventTimestamp: java.sql.Timestamp)

object TruckSpeedEvent {
  val eventTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def fromJson(on: ObjectNode): TruckSpeedEvent = {
    TruckSpeedEvent(
      on.findValue("eventTime").asText(), on.findValue("eventSource").asText(), 
      on.findValue("truckId").intValue(),
      on.findValue("driverId").intValue(), on.findValue("driverName").asText(), on.findValue("routeId").intValue(),
      on.findValue("route").asText(), on.findValue("speed").intValue(),
      new java.sql.Timestamp(eventTimeFormat.parse(on.findValue("eventTime").asText()).getTime))
  }
}
