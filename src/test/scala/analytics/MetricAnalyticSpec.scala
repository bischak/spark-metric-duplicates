package analytics

/**
  * Created by Dmytro Bischak on 4/27/17.
  */

import java.sql
import java.sql.Timestamp
import java.util.UUID

import storage.Events.Record
import storage.Stats.{MetricDuplicates, RecordMetricDuplicates}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._
import utils.SpecSparkContext


class MetricAnalyticSpec extends FlatSpec with BeforeAndAfter with SpecSparkContext with GivenWhenThen with Matchers {

  override def appName = "MetricAnalytic"

  override def master = "local[2]"

  override def batchDuration = Seconds(2)

  def uniqueRecordId = UUID.randomUUID().toString

  "MetricAnalytic" should "empty data" in {

    When("empty RDD")

    val records = sc.parallelize(Seq.empty[Record])

    val time: Timestamp = new Timestamp(DateTime.now(DateTimeZone.UTC).getMillis)
    val day = new Timestamp(DateTime.now(DateTimeZone.UTC).plusDays(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[RecordMetricDuplicates] = MetricAnalytic.recordDuplicates(records, time, day)

    Then("empty duplicates")
    metricStats.collect() should equal(Array())

  }

  it should "one record no duplicates" in {

    Given("unique metric events")

    val eventTime = DateTime.now(DateTimeZone.UTC)

    val events = Seq(
      Record(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      Record(
        record = "rec1",
        metric = "B",
        time = eventTime.plusMinutes(2).getMillis
      ),
      Record(
        record = "rec1",
        metric = "C",
        time = eventTime.plusMinutes(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[Record] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)
    val day = new Timestamp(eventTime.plusHours(2).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[RecordMetricDuplicates] = MetricAnalytic.recordDuplicates(records, time, day)

    Then("no duplicates")
    metricStats.collect().toSet should equal(Set(
      RecordMetricDuplicates(record = "rec1", metric = "A", day = day.getTime, dup = 0),
      RecordMetricDuplicates(record = "rec1", metric = "B", day = day.getTime, dup = 0),
      RecordMetricDuplicates(record = "rec1", metric = "C", day = day.getTime, dup = 0)
    ))

  }


  it should "one record with duplicates" in {
    Given("repeated metric events in time")

    val eventTime = DateTime.now(DateTimeZone.UTC)

    val events = Seq(
      Record(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      Record(
        record = "rec1",
        metric = "B",
        time = eventTime.plusMinutes(2).getMillis
      ),
      Record(
        record = "rec1",
        metric = "B",
        time = eventTime.plusMinutes(3).getMillis
      ),
      Record(
        record = "rec1",
        metric = "B",
        time = eventTime.plusHours(2).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[Record] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)
    val day = new Timestamp(eventTime.plusDays(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[RecordMetricDuplicates] = MetricAnalytic.recordDuplicates(records, time, day)

    Then("duplicates by one record")
    metricStats.collect().toSet should equal(Set(
      RecordMetricDuplicates(record = "rec1", metric = "A", day = day.getTime, dup = 0),
      RecordMetricDuplicates(record = "rec1", metric = "B", day = day.getTime, dup = 1)
    ))
  }

  it should "multiple records no duplicates" in {
    Given("unique metric events for different records")

    val eventTime = DateTime.now(DateTimeZone.UTC)
    val rec1 = uniqueRecordId
    val rec2 = uniqueRecordId
    val rec3 = uniqueRecordId
    val rec4 = uniqueRecordId

    val events = Seq(
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.getMillis
      ),
      Record(
        record = rec2,
        metric = "B",
        time = eventTime.plusMinutes(1).getMillis
      ),
      Record(
        record = rec3,
        metric = "A",
        time = eventTime.plusMinutes(2).getMillis
      ),
      Record(
        record = rec4,
        metric = "B",
        time = eventTime.plusHours(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[Record] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)
    val day = new Timestamp(eventTime.plusDays(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[RecordMetricDuplicates] = MetricAnalytic.recordDuplicates(records, time, day)

    Then("no duplicates")
    metricStats.collect().toSet should equal(Set(
      RecordMetricDuplicates(record = rec1, metric = "A", day = day.getTime, dup = 0),
      RecordMetricDuplicates(record = rec2, metric = "B", day = day.getTime, dup = 0),
      RecordMetricDuplicates(record = rec3, metric = "A", day = day.getTime, dup = 0),
      RecordMetricDuplicates(record = rec4, metric = "B", day = day.getTime, dup = 0)
    ))
  }

  it should "multiple records with duplicates" in {

    Given("repeated metric events")

    val eventTime = DateTime.now(DateTimeZone.UTC)
    val rec1 = uniqueRecordId
    val rec2 = uniqueRecordId

    val events = Seq(
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.getMillis
      ),
      Record(
        record = rec2,
        metric = "B",
        time = eventTime.plusMinutes(1).getMillis
      ),
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.plusMinutes(2).getMillis
      ),
      Record(
        record = rec2,
        metric = "B",
        time = eventTime.plusMinutes(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[Record] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)
    val day = new Timestamp(eventTime.plusDays(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[RecordMetricDuplicates] = MetricAnalytic.recordDuplicates(records, time, day)

    Then("duplicates in both records")
    metricStats.collect().toSet should equal(Set(
      RecordMetricDuplicates(record = rec1, metric = "A", day = day.getTime, dup = 1),
      RecordMetricDuplicates(record = rec2, metric = "B", day = day.getTime, dup = 1)
    ))
  }

  it should "duplicates in time range" in {

    val eventTime = DateTime.now(DateTimeZone.UTC)
    val rec1 = uniqueRecordId

    Given("range of the same metrics")
    val events = Seq(
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.getMillis
      ),
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.plusMinutes(1).getMillis
      ),
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.plusMinutes(2).getMillis
      ),
      Record(
        record = rec1,
        metric = "A",
        time = eventTime.plusMinutes(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[Record] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusMinutes(2).getMillis)
    val day = new Timestamp(eventTime.plusDays(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[RecordMetricDuplicates] = MetricAnalytic.recordDuplicates(records, time, day)

    Then("in duplicates are only metrics occurred before calculation time")
    metricStats.collect().toSet should equal(Set(
      RecordMetricDuplicates(record = rec1, metric = "A", day = day.getTime, dup = 1)
    ))
  }

  it should "aggregate one metric from pre-calculated data" in {

    val timeMillis = DateTime.now(DateTimeZone.UTC).toLocalDate.toDate.getTime

    Given("range of stats for one metric")
    val stats = Seq(
      RecordMetricDuplicates(record = "rec1", metric = "A", day = timeMillis, dup = 1),
      RecordMetricDuplicates(record = "rec2", metric = "A", day = timeMillis, dup = 2)
    )

    val rdd = sc.parallelize(stats)

    val actual = MetricAnalytic.duplicatesAllMetrics(rdd, timeMillis)

    Then("one metric for this day")
    actual.toSet should equal(Set(
      MetricDuplicates(metric = "A", day = timeMillis, dup = 3)
    ))
  }

  it should "aggregate of several metrics from pre-calculated data" in {

    val timeMillis = DateTime.now(DateTimeZone.UTC).toLocalDate.toDate.getTime

    Given("range of stats for several metrics")
    val stats = Seq(
      RecordMetricDuplicates(record = "rec1", metric = "A", day = timeMillis, dup = 1),
      RecordMetricDuplicates(record = "rec2", metric = "A", day = timeMillis, dup = 5),
      RecordMetricDuplicates(record = "rec1", metric = "B", day = timeMillis, dup = 8),
      RecordMetricDuplicates(record = "rec2", metric = "B", day = timeMillis, dup = 4),
      RecordMetricDuplicates(record = "rec2", metric = "D", day = timeMillis, dup = 0),
      RecordMetricDuplicates(record = "rec4",
        metric = "D",
        day = new DateTime(timeMillis).minusDays(1).getMillis,
        dup = 10)
    )

    val rdd = sc.parallelize(stats)

    val actual = MetricAnalytic.duplicatesAllMetrics(rdd, timeMillis)

    Then("several metrics only for this day")
    actual.toSet should equal(Set(
      MetricDuplicates(metric = "A", day = timeMillis, dup = 6),
      MetricDuplicates(metric = "B", day = timeMillis, dup = 12),
      MetricDuplicates(metric = "D", day = timeMillis, dup = 10)
    ))
  }


  it should "aggregate one metric from raw data" in {

    val eventTime = DateTime.now(DateTimeZone.UTC)

    Given("range of records for one metric")
    val stats = Seq(
      Record(record = "rec1", time = eventTime.getMillis, metric = "A"),
      Record(record = "rec2", time = eventTime.plusHours(1).getMillis, metric = "A"),
      Record(record = "rec2", time = eventTime.plusHours(2).getMillis, metric = "A")
    )

    val analyticTime = new Timestamp(eventTime.plusHours(3).getMillis)

    val rdd = sc.parallelize(stats)

    Then("one metric for this 3 hours time")
    MetricAnalytic.duplicatesOfMetric(rdd, metric = "A", analyticTime, analyticTime) should equal(
      MetricDuplicates(metric = "A", day = analyticTime.getTime, dup = 1)
    )
  }

  it should "aggregate of several metrics from raw data" in {

    val eventTime1 = DateTime.now(DateTimeZone.UTC)
    val eventTime2 = eventTime1.plusMinutes(10)
    val eventTime3 = eventTime1.plusMinutes(20)
    val eventTime4 = eventTime1.plusMinutes(50)
    val analyticTime = eventTime1.plusMinutes(40)

    Given("range of records for several metrics")
    val stats = Seq(
      // 1
      Record(record = "rec1", time = eventTime1.getMillis, metric = "A"),
      Record(record = "rec1", time = eventTime2.getMillis, metric = "A"),
      Record(record = "rec1", time = eventTime3.getMillis, metric = "B"),
      //2
      Record(record = "rec2", time = eventTime1.getMillis, metric = "A"),
      Record(record = "rec2", time = eventTime2.getMillis, metric = "A"),
      Record(record = "rec2", time = eventTime3.getMillis, metric = "A"),
      Record(record = "rec2", time = eventTime4.getMillis, metric = "A"),
      // 0
      Record(record = "rec4", time = eventTime1.getMillis, metric = "D"),
      Record(record = "rec4", time = eventTime2.getMillis, metric = "B")
    )

    val rdd = sc.parallelize(stats)

    val analyticTimestamp = new Timestamp(analyticTime.getMillis)
    val actual = MetricAnalytic.duplicatesAllMetrics(rdd, analyticTimestamp, analyticTimestamp)

    Then("several metrics only for this time range")
    actual.toSet should equal(Set(
      MetricDuplicates(metric = "A", day = analyticTime.getMillis, dup = 3),
      MetricDuplicates(metric = "B", day = analyticTime.getMillis, dup = 0),
      MetricDuplicates(metric = "D", day = analyticTime.getMillis, dup = 0)
    ))
  }

}
