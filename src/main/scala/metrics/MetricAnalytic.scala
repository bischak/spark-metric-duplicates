package metrics

import java.sql.Timestamp

import org.apache.spark.rdd.RDD

import scalaz.Scalaz._


/**
  * Created by Dmytro Bischak on 4/24/17.
  */
object MetricAnalytic {

  /*
  * Duplicates for all metrics for time less than @time
  * */
  def duplicates(records: RDD[MetricEvent], time: Timestamp): RDD[MetricRecordStat] = {

    val recordsPairRDD: RDD[(String, MetricEvent)] = records.map(row => row.record -> row)

    // Calculate count of every metric for all records
    val countRDD: RDD[(String, Map[String, Int])] =
      recordsPairRDD
        .aggregateByKey(Map.empty[String, Int])(seqOp = {
          case (acc: Map[String, Int], metric: MetricEvent) =>

            val isBefore = new Timestamp(metric.time).before(time)
            val count: Int = acc.getOrElse(metric.metric, 0) + (if (isBefore) 1 else 0)
            val newCount: (String, Int) = metric.metric -> count

            acc + newCount

        }, combOp = {
          case (m1, m2) =>
            // Add sum values in Map's by key
            m1 |+| m2
        })

    // Duplicates is just Count - 1
    countRDD.flatMap {
      case (record, metricsCount: Map[String, Int]) =>

        metricsCount.map {
          case (metric, count) =>

            val duplicates = math.max(0, count - 1)

            MetricRecordStat(
              record = record,
              metric = metric,
              day = time.getTime,
              dup = duplicates
            )

        }

    }

  }

}
