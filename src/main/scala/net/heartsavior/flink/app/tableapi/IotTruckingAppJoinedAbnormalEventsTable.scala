package net.heartsavior.flink.app.tableapi

import java.util.Properties

import net.heartsavior.flink.datasource.EventDataSources
import net.heartsavior.flink.utils.IotTruckingAppConf
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object IotTruckingAppJoinedAbnormalEventsTable {
  // this is simplified version of Streaming-Analytics-Trucking-Ref-App to make it compatible with
  // Flink Table API

  def main(args: Array[String]): Unit = {

    val conf = new IotTruckingAppConf(args)

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(300000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // need to avoid conflict fields between two streams while joining...
    val geoTable: Table = tableEnv.fromTableSource(
      EventDataSources.geoTableSource(conf.brokers(), conf.geoEventsTopic()))
      .select('eventTime as 'geo_eventTime, 'eventSource as 'geo_eventSource,
        'truckId as 'geo_truckId, 'driverId as 'geo_driverId,
        'driverName as 'geo_driverName, 'routeId as 'geo_routeId,
        'eventType as 'geo_eventType, 'latitude as 'geo_latitude,
        'longitude as 'geo_longitude, 'correlationId as 'geo_correlationId)

    val speedTable: Table = tableEnv.fromTableSource(
      EventDataSources.speedTableSource(conf.brokers(), conf.speedEventsTopic()))

    val joined: Table = geoTable
      .join(speedTable).where(
        """
        geo_driverId = driverId
        |&& geo_truckId = truckId
        |&& geo_eventTime >= eventTime
        |&& geo_eventTime < eventTime + 1.seconds
        """.stripMargin)

    val outTable = joined
      .where('geo_eventType !== "Normal")
      .select('driverId, 'driverName, 'route, 'speed)

    val sinkProps = new Properties()
    sinkProps.setProperty("bootstrap.servers", conf.brokers())

    outTable.writeToSink(new Kafka010JsonTableSink(conf.outputTopic(), sinkProps))

    env.execute("IotTruckingAppJoinedAbnormalEventsTable")
  }
}
