package app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneOffset
import java.util.{Date, TimeZone}

import analytics.MetricAnalytic
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.joda.time.{DateTime, DateTimeZone}
import storage.{MetricStorage, Stats, StatsStorage}
import utils.SparkUtils._

import scala.util.Try

/**
  * Created by Dmytro Bischak on 5/5/17.
  *
  *
  * Aggregating duplicates for certain date by one or all metrics
  *
  */
object MetricsAggregatorApp {

  private val LOGGER_NAME = "MetricsAggregator"

  @transient lazy val log = LogManager.getLogger(LOGGER_NAME)

  private val dateFormat = {
    val format = new SimpleDateFormat("dd.MM.yyyy")
    format.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC.getId))
    format
  }

  private def parseDate(str: String): Option[Date] = Try {
    Some(dateFormat.parse(str))
  }.getOrElse(None)

  def main(args: Array[String]): Unit = {

    args match {
      case Array(date) =>
        runApp(date, None)
      case Array(date, metric) =>
        runApp(date, Some(metric))
      case _ =>
        log.info(
          """Application args: <date> <metricID>
            |<date> - aggregate metric duplicates for certain date (date format is "dd.MM.yyyy")
            |<metricID> - optional parameter, calculate metric duplicates only for this metric
            |Example: 20.01.2017 e55c31ae-b965-41a7-a4c3-b8c4178dedd3
          """.stripMargin)
    }


    def runApp(dateStr: String, metricIdOpt: Option[String]): Unit = {

      parseDate(dateStr).foreach {
        date =>

          log.info("Starting MetricsAggregator")
          log.info(s"Aggregation for $date ${metricIdOpt.getOrElse("")}")

          val config: Config = ConfigFactory.load("aggregator.conf")

          val sparkConf = toSparkConfig(config)
            .setAppName(config.getString("app.name"))

          implicit val sc = SparkContext.getOrCreate(sparkConf)

          // Start of Tomorrow, look for previous data
          val startOfNextDay = new Timestamp(new DateTime(date.getTime, DateTimeZone.UTC).plusDays(1).getMillis)

          val analyticDay = new Timestamp(date.getTime)

          metricIdOpt match {
            case Some(metricId) =>

              val duplicate: Stats.MetricDuplicates = MetricAnalytic.duplicatesOfMetric(
                MetricStorage.all(),
                metricId,
                startOfNextDay,
                analyticDay
              )

              StatsStorage.writeMetricDuplicates(sc.parallelize(Seq(
                duplicate
              )))

            case None =>

              val duplicates = MetricAnalytic.duplicatesAllMetrics(
                MetricStorage.all(),
                startOfNextDay,
                analyticDay
              )

              StatsStorage.writeMetricDuplicates(sc.parallelize(duplicates))

          }

      }

    }

  }

}
