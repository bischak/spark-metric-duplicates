package app

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import storage.Events.Record
import storage.MetricStorage
import utils.SparkUtils._

import scala.collection.JavaConverters._

/**
  * Created by Dmytro Bischak on 4/24/17.
  *
  *
  * Collecting metrics from Kafka stream. Recalculation metrics duplicates for record level.
  *
  */
object MetricsCollectorApp {

  val LOGGER_NAME = "MetricsCollector"

  @transient lazy val log = LogManager.getLogger(LOGGER_NAME)

  def getKafkaParams(config: Config): Map[String, Object] = {
    config.getConfig("kafka").entrySet()
      .asScala.map(
      kv => kv.getKey -> kv.getValue.unwrapped()
    ).map {
      case (key, value: String) if key.endsWith(".deserializer") => key -> Class.forName(value)
      case kv => kv
    }.toMap
  }

  def main(args: Array[String]): Unit = {

    log.info("Starting MetricsCollector")

    val config: Config = ConfigFactory.load("collector.conf")

    val sparkConf = toSparkConfig(config)
      .setAppName(config.getString("app.name"))

    val batchDuration = Milliseconds(config.getDuration("app.batchDuration", TimeUnit.MILLISECONDS))

    val streamingContext = new StreamingContext(sparkConf, batchDuration)

    val topics = config.getStringList("app.topics").asScala
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, getKafkaParams(config))
    )

    // Stream of Kafka data without offsets
    val result: DStream[String] = stream.map(_.value)

    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(streamingContext.sparkContext)

    import sqlContext.implicits._

    implicit val sc = sqlContext.sparkContext

    result.foreachRDD {
      (rdd: RDD[String]) =>

        rdd.cache()

        if (rdd.count() > 0) {

          val metricsRDD = sqlContext.read.json(rdd).as[Record].rdd

          metricsRDD.cache()

          if (metricsRDD.count() > 0) {

            MetricStorage.write(metricsRDD)

            log.info(s"Found JSON here: ${metricsRDD.collect().toSeq}")

          }

        }


    }

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}
