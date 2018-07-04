package net.heartsavior.flink.app.tableapi

import java.util.Properties

import net.heartsavior.flink.datasource.EventDataSources
import net.heartsavior.flink.utils.IotTruckingAppConf
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object IotTruckingAppDistinctPairDriverAndTruckTable {

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
        .select('driverId, 'truckId)
        .distinct()

    val sinkProps = new Properties()
    sinkProps.setProperty("bootstrap.servers", conf.brokers())

    // Kafka 010 TableSink requires Append mode whereas above query requires retract mode
    // outTable.writeToSink(new Kafka010JsonTableSink(conf.outputTopic(), sinkProps))
    // printing table instead... table sinks which supports upsert should work

    // below doesn't work as below line implicitly converts table as 'append stream'
    // via org.apache.flink.table.api.scala.package$.table2RowDataStream
    // though we are calling toRetractStream
    //outTable.toRetractStream[Row](outTable.dataType).print()

    implicit val typeInfo = Types.ROW(outTable.getSchema.getColumnNames,
      outTable.getSchema.getTypes)
    tableEnv.toRetractStream(outTable).print()

    env.execute("IotTruckingAppDistinctPairDriverAndTruckTable")
  }
}
