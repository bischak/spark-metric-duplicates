package analytics

import java.sql.Timestamp

import storage.Events.Record
import storage.Stats.{MetricDuplicates, RecordMetricDuplicates}
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._


/**
  * Created by Dmytro Bischak on 4/24/17.
  */
object MetricAnalytic {

  /**
    * Duplicates for all metrics for certain time
    *
    * @param rdd         pre-calculated duplicates for certain record and metric
    * @param analyticDay analytic date of results
    *
    */
  def duplicatesAllMetrics(rdd: RDD[RecordMetricDuplicates], analyticDay: Long): Seq[MetricDuplicates] = {

    // process all records, complexity is O(N)
    rdd
      .aggregate(Map.empty[String, Int])(
        seqOp = {
          case (acc: Map[String, Int], stat: RecordMetricDuplicates) =>

            val dup: Int = acc.getOrElse(stat.metric, 0) + stat.dup
            val result = stat.metric -> dup
            acc + result
        },
        combOp = {
          case (m1, m2) =>
            // sum values in Map's by key
            m1 |+| m2
        }).map {
      case (metric, dup) => MetricDuplicates(
        metric,
        analyticDay,
        dup
      )
    }.toSeq

  }


  /**
    * Duplicates for every record for all metrics for time less than @beforeTime
    *
    * @param records     source data
    * @param beforeTime  time filter
    * @param analyticDay analytic date of results
    *
    */
  def recordDuplicates(records: RDD[Record], beforeTime: Timestamp, analyticDay: Timestamp): RDD[RecordMetricDuplicates] = {

    val recordsPairRDD: RDD[(String, Record)] = records.map(row => row.record -> row)

    // Calculate count of every metric for all records
    val countRDD: RDD[(String, Map[String, Int])] =
      recordsPairRDD
        .aggregateByKey(Map.empty[String, Int])(seqOp = {
          case (acc: Map[String, Int], metric: Record) =>

            val isBefore = new Timestamp(metric.time).before(beforeTime)
            val count: Int = acc.getOrElse(metric.metric, 0) + (if (isBefore) 1 else 0)
            val newCount: (String, Int) = metric.metric -> count

            acc + newCount

        }, combOp = {
          case (m1, m2) =>
            // sum values in Map's by key
            m1 |+| m2
        })

    // Duplicate is just Count - 1
    countRDD.flatMap {
      case (record, metricsCount: Map[String, Int]) =>

        metricsCount.map {
          case (metric, count) =>

            val duplicates = math.max(0, count - 1)

            RecordMetricDuplicates(
              record = record,
              metric = metric,
              day = analyticDay.getTime,
              dup = duplicates
            )

        }

    }

  }

  /**
    * Duplicates for every record for all metrics for time less than @beforeTime
    *
    * @param records     source data
    * @param beforeTime  time filter
    * @param analyticDay analytic date of results
    **/
  def duplicatesAllMetrics(records: RDD[Record], beforeTime: Timestamp, analyticDay: Timestamp): Seq[MetricDuplicates] = {

    val recordsPairRDD: RDD[(String, Record)] = records.map(row => row.record -> row)

    // Calculate count of every metric for all records
    // (record, (metric, count))
    val countRDD: RDD[(String, Map[String, Int])] =
    recordsPairRDD
      .aggregateByKey(Map.empty[String, Int])(seqOp = {
        case (acc: Map[String, Int], metric: Record) =>

          val isBefore = new Timestamp(metric.time).before(beforeTime)
          val count: Int = acc.getOrElse(metric.metric, 0) + (if (isBefore) 1 else 0)
          val newCount: (String, Int) = metric.metric -> count

          acc + newCount

      }, combOp = {
        case (m1, m2) =>
          // sum values in Map's by key
          m1 |+| m2
      })


    // (metric, duplicates)
    countRDD.aggregate(Map.empty[String, Int])(
      seqOp = {
        case (acc: Map[String, Int], (recordId: String, metricsCount: Map[String, Int])) =>

          // Duplicate is just Count - 1
          val duplicates: Map[String, Int] = metricsCount.map {
            case (metric, count) =>
              metric -> math.max(0, count - 1)
          }

          acc |+| duplicates
      }, combOp = {
        case (m1, m2) =>
          // sum values in Map's by key
          m1 |+| m2
      }
    ).map {
      case (metric: String, duplicates: Int) =>
        MetricDuplicates(metric, analyticDay.getTime, duplicates)
    }.toSeq

  }

  /**
    * Duplicates for every record for all metrics for time less than @beforeTime
    *
    * @param records     source data
    * @param metric      ID of metric
    * @param beforeTime  time filter
    * @param analyticDay analytic date of results
    *
    **/
  def duplicatesOfMetric(records: RDD[Record], metric: String, beforeTime: Timestamp, analyticDay: Timestamp): MetricDuplicates = {

    val recordsPairRDD: RDD[(String, Record)] = records
      .filter(_.metric == metric)
      .map(row => row.record -> row)

    // Calculate count for all records
    // (record, count)
    val countRDD: RDD[(String, Int)] =
    recordsPairRDD
      .aggregateByKey(0)(seqOp = {
        case (acc: Int, metric: Record) =>

          val isBefore = new Timestamp(metric.time).before(beforeTime)
          val count: Int = acc + (if (isBefore) 1 else 0)

          count

      }, combOp = {
        case (count1, count2) =>
          count1 + count2
      })

    val duplicates: Int = countRDD.aggregate(0)(
      seqOp = {
        case (acc: Int, (recordId: String, count: Int)) =>

          // Duplicate is just Count - 1
          val duplicates = math.max(0, count - 1)

          acc + duplicates
      }, combOp = {
        case (count1, count2) =>
          count1 + count2
      }
    )

    MetricDuplicates(metric, analyticDay.getTime, duplicates)
  }


}
