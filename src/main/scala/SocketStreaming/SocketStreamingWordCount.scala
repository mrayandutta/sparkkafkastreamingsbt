package SocketStreaming

import org.apache.spark._
import org.apache.spark.streaming._
import utility.LogUtil // not necessary in Spark 1.3+

object SocketStreamingWordCount {

  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val wordDstream = words.map(word => (word, 1))
    val wordCounts = wordDstream.reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      LogUtil.logger.error(s"word :$word,sum:$sum")
      val output = (word, sum)
      LogUtil.logger.error(s"output :$output")
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc))//.initialState(initialRDD))
    stateDstream.print()

    //Line to add hadoop home dir
    //System.setProperty("hadoop.home.dir", "c://winutils")
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
