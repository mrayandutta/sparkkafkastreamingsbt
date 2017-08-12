package kafkastreaming

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.serializer.StringDecoder

/**
  * Created by mrpiku2017 on 8/9/2017.
  */
object KafkaWordCountStatefulWithDirectStream
{
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topicsSet = List("wordcount").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    val words = kafkaStream.flatMap(x =>  x._2.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).updateStateByKey(updateFunc)

    //kafkaStream.print()  //prints the stream of data received
    wordCounts.print()   //prints the wordcount result of the stream

    //System.setProperty("hadoop.home.dir", "c://winutils")
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


  }


}
