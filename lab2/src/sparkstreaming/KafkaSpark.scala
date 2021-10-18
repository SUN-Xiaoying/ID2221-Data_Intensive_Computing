package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

object KafkaSpark {
  def main(args: Array[String]) {

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Key_Avg")	
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")

    val topics = Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, topics
    )
    ssc.checkpoint("/home/hdoop/ID2221/lab2/src/sparkstreaming/checkponit/")

    // value Sample: value=p,15,
    val pairs = messages.map(data => (data._2.split(",")(0),data._2.split(",")(1).toDouble))
    pairs.print(10)

    // measure the average value for each key in a stateful manner
   // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double,Int)]):(String, Double) = {
        val newValue = value.getOrElse(0.0)
        val oldState = state.getOption.getOrElse((0.0, 0))
        val averageLength = oldState._2 + 1
        val sumValues = oldState._1 + newValue
        val newAverage = (sumValues / averageLength)
        state.update((sumValues, averageLength))
        (key, newAverage)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
