package net.heartsavior.flink.utils

import org.rogach.scallop.ScallopConf

class IotTruckingAppConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val brokers = opt[String](name = "brokers", required = true, noshort = true)
  val checkpoint = opt[String](name = "checkpoint", required = true, noshort = true)

  val geoEventsTopic = opt[String](name = "geo-events-topic", required = true, default = Some(IotTruckingAppConf.DEFAULT_GEO_EVENTS_TOPIC), noshort = true)
  val speedEventsTopic = opt[String](name = "speed-events-topic", required = true, default = Some(IotTruckingAppConf.DEFAULT_SPEED_EVENTS_TOPIC), noshort = true)
  val queryStatusTopic = opt[String](name = "query-status-topic", required = false, default = Some(IotTruckingAppConf.DEFAULT_APP_QUERY_STATUS_TOPIC), noshort = true)
  val outputTopic = opt[String](name = "output-topic", required = true, noshort = true)

  verify()
}

object IotTruckingAppConf {
  val DEFAULT_GEO_EVENTS_TOPIC = "truck_events_stream"
  val DEFAULT_SPEED_EVENTS_TOPIC = "truck_speed_events_stream"
  val DEFAULT_APP_QUERY_STATUS_TOPIC = "app_query_progress"
}