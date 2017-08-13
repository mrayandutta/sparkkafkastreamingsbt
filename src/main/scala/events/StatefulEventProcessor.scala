package events

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import utility.LogUtil

/**
  * Created by mrpiku2017 on 8/10/2017.
  */
object StatefulEventProcessor
{


  def createEvent(strEvent: String): PerformanceEvent =
  {
    LogUtil.logger.error("Event String "+strEvent)
    val eventData = strEvent.split('|')

    val instanceId = eventData(0).toInt
    val time = eventData(1).toLong
    val utilization = eventData(2).toDouble

    new PerformanceEvent(instanceId, time, utilization)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("StatefulEventProcessor")
      .setMaster("local[*]")
      .set("spark.driver.memory", "2g")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.backpressure.enabled", "true")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("C:/checkpoint/") // set checkpoint directory

    val topicName = "events"

    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092","auto.offset.reset" -> "smallest")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, scala.collection.immutable.Set(topicName))
    val messages = kafkaStream.flatMap(x =>  x._2.split("|"))
    messages.print(10)
    LogUtil.logger.error("Kafka Stream  "+kafkaStream)

    /*val nonFilteredEvents = kafkaStream.map((tuple) => createEvent(tuple._2))

    val events = nonFilteredEvents.filter((event) => {
      event.highUtiization() && event.isTimeRelevant()
    })

    val groupedEvents = events.transform((rdd) => rdd.groupBy(_.instanceId))

    //mapWithState function
    val updateState = (batchTime: Time, key: Int, value: Option[Iterable[PerformanceEvent]], state: State[(Option[Long], Set[PerformanceEvent])]) => {

      if (!state.exists) state.update((None, Set.empty))

      var updatedSet = Set[PerformanceEvent]()
      value.get.foreach(updatedSet.add(_))

      //exclude non-relevant events
      state.get()._2.foreach((tempEvent) => {
        if (tempEvent.isTimeRelevant()) updatedSet.add(tempEvent)
      })

      var lastAlertTime = state.get()._1

      //launch alert if no alerts launched yet or if last launched alert was more than 120 seconds ago
      if (updatedSet.size >= 2 && (lastAlertTime.isEmpty || !((System.currentTimeMillis() - lastAlertTime.get) <= 120000)))
      {
        lastAlertTime = Some(System.currentTimeMillis())
        //alert in json to be published to kafka
      }

      state.update((lastAlertTime, updatedSet))
      Some((key, updatedSet)) // mapped value
    }

    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = groupedEvents.mapWithState(spec)

    mappedStatefulStream.print()
*/
    ssc.start()
    ssc.awaitTermination()
  }
}
