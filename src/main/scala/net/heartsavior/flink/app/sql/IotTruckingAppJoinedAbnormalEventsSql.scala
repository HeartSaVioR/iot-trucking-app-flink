package net.heartsavior.flink.app.sql

import java.util.Properties

import net.heartsavior.flink.datasource.EventDataSources
import net.heartsavior.flink.utils.IotTruckingAppConf
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink
import org.apache.flink.table.api._

object IotTruckingAppJoinedAbnormalEventsSql {
  // this is simplified version of Streaming-Analytics-Trucking-Ref-App to make it compatible with
  // Flink SQL

  def main(args: Array[String]): Unit = {

    val conf = new IotTruckingAppConf(args)

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(300000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.registerTableSource("geo",
      EventDataSources.geoTableSource(conf.brokers(), conf.geoEventsTopic()))
    tableEnv.registerTableSource("speed",
      EventDataSources.speedTableSource(conf.brokers(), conf.speedEventsTopic()))

    val outTable = tableEnv.sqlQuery(
      """
        |SELECT
        | g.driverId as driverId, g.driverName AS driverName,
        | s.route AS route, s.speed AS speed
        |FROM geo AS g
        |JOIN speed AS s on
        |g.driverId = s.driverId
        |AND g.truckId = s.truckId
        |AND g.eventTime BETWEEN s.eventTime AND s.eventTime + INTERVAL '1' SECOND
        |WHERE g.eventType <> 'Normal'
      """.stripMargin)

    val sinkProps = new Properties()
    sinkProps.setProperty("bootstrap.servers", conf.brokers())

    outTable.writeToSink(new Kafka010JsonTableSink(conf.outputTopic(), sinkProps))

    env.execute("IotTruckingAppJoinedAbnormalEventsSql")
  }
}
