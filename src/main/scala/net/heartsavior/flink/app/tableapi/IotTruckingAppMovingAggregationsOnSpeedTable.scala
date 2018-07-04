package net.heartsavior.flink.app.tableapi

import java.util.Properties

import net.heartsavior.flink.datasource.EventDataSources
import net.heartsavior.flink.utils.IotTruckingAppConf
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object IotTruckingAppMovingAggregationsOnSpeedTable {

  def main(args: Array[String]): Unit = {

    val conf = new IotTruckingAppConf(args)

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(300000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val speedTable: Table = tableEnv.fromTableSource(
      EventDataSources.speedTableSource(conf.brokers(), conf.speedEventsTopic()))

    val outTable = speedTable
      .window(Slide over 1.minutes every 10.seconds on 'eventTime as 'w)
      .groupBy('driverId, 'w)
      .select('w.start as 'window_start, 'w.end as 'window_end, 'driverId,
    'speed.max as 'max_speed, 'speed.min as 'min_speed, 'speed.avg as 'avg_speed)

    val sinkProps = new Properties()
    sinkProps.setProperty("bootstrap.servers", conf.brokers())

    outTable.writeToSink(new Kafka010JsonTableSink(conf.outputTopic(), sinkProps))

    env.execute("IotTruckingAppMovingAggregationsOnSpeedTable")
  }
}
