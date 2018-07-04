package net.heartsavior.flink.app.sql

import java.util.Properties

import net.heartsavior.flink.utils.{IotTruckingAppConf, TruckSpeedSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink
import org.apache.flink.table.api._
import org.apache.flink.table.sources.TableSource
import org.apache.flink.types.Row

object IotTruckingAppMovingAggregationsOnSpeedSql {

  def main(args: Array[String]): Unit = {

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(300000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val speedTableSource: TableSource[Row] = new TruckSpeedSource(conf.brokers(),
      conf.speedEventsTopic())
    tableEnv.registerTableSource("speed", speedTableSource)

    val outTable = tableEnv.sqlQuery(
      """
        |SELECT
        | HOP_START(eventTimestamp, INTERVAL '1' minute, INTERVAL '10' second) AS window_start,
        | HOP_END(eventTimestamp, INTERVAL '1' minute, INTERVAL '10' second) AS window_end,
        | driverId, max(speed) AS max_speed, min(speed) AS min_speed, AVG(speed) AS avg_speed
        |FROM speed
        |GROUP BY HOP(eventTimestamp, INTERVAL '1' minute, INTERVAL '10' second), driverId
      """.stripMargin)

    val sinkProps = new Properties()
    sinkProps.setProperty("bootstrap.servers", conf.brokers())

    outTable.writeToSink(new Kafka010JsonTableSink(conf.outputTopic(), sinkProps))

    env.execute("IotTruckingAppMovingAggregationsOnSpeedSql")

    // TODO: 'No watermark' is showing in Flink UI - is it a bug? or am I missing something?
  }
}
