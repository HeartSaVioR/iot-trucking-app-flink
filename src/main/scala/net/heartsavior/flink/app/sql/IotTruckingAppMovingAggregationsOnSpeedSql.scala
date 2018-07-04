package net.heartsavior.flink.app.sql

import java.util.Properties

import net.heartsavior.flink.datasource.EventDataSources
import net.heartsavior.flink.utils.IotTruckingAppConf
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink
import org.apache.flink.table.api._

object IotTruckingAppMovingAggregationsOnSpeedSql {

  def main(args: Array[String]): Unit = {

    val conf = new IotTruckingAppConf(args)

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(300000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.registerTableSource("speed",
      EventDataSources.speedTableSource(conf.brokers(), conf.speedEventsTopic()))

    val outTable = tableEnv.sqlQuery(
      """
        |SELECT
        | HOP_START(eventTime, INTERVAL '1' minute, INTERVAL '10' second) AS window_start,
        | HOP_END(eventTime, INTERVAL '1' minute, INTERVAL '10' second) AS window_end,
        | driverId, max(speed) AS max_speed, min(speed) AS min_speed, AVG(speed) AS avg_speed
        |FROM speed
        |GROUP BY HOP(eventTime, INTERVAL '1' minute, INTERVAL '10' second), driverId
      """.stripMargin)

    val sinkProps = new Properties()
    sinkProps.setProperty("bootstrap.servers", conf.brokers())

    outTable.writeToSink(new Kafka010JsonTableSink(conf.outputTopic(), sinkProps))

    env.execute("IotTruckingAppMovingAggregationsOnSpeedSql")
  }
}
