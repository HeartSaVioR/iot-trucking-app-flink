package net.heartsavior.flink.datasource

import java.text.SimpleDateFormat

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

case class TruckGeoEvent(eventTime: java.lang.String, eventSource: java.lang.String,
                         truckId: java.lang.Integer, driverId: java.lang.Integer,
                         driverName: java.lang.String, routeId: java.lang.Integer,
                         route: java.lang.String, eventType: java.lang.String,
                         latitude: java.lang.Double, longitude: java.lang.Double,
                         correlationId: java.lang.Double,
                         eventTimestamp: java.sql.Timestamp)

object TruckGeoEvent {
  val eventTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def fromJson(on: ObjectNode): TruckGeoEvent = {
    TruckGeoEvent(
      on.findValue("eventTime").asText(), on.findValue("eventSource").asText(), on.findValue("truckId").intValue(),
      on.findValue("driverId").intValue(), on.findValue("driverName").asText(), on.findValue("routeId").intValue(),
      on.findValue("route").asText(), on.findValue("eventType").asText(), on.findValue("latitude").doubleValue(),
      on.findValue("longitude").doubleValue(), on.findValue("correlationId").doubleValue(),
      new java.sql.Timestamp(eventTimeFormat.parse(on.findValue("eventTime").asText()).getTime))
  }
}
