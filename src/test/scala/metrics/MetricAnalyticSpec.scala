package metrics

/**
  * Created by Dmytro Bischak on 4/27/17.
  */

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.joda.time.DateTime
import org.scalatest._
import utils.SpecSparkContext


class MetricAnalyticSpec extends FlatSpec with BeforeAndAfter with SpecSparkContext with GivenWhenThen with Matchers {

  override def appName = "MetricAnalytic"

  override def master = "local[2]"

  override def batchDuration = Seconds(2)

  "DuplicatesAnalytic" should "empty data" in {

    When("empty RDD")

    val records = sc.parallelize(Seq.empty[MetricEvent])

    val time: Timestamp = new Timestamp(DateTime.now().getMillis)

    When("duplicates calculation")
    val metricStats: RDD[MetricRecordStat] = MetricAnalytic.duplicates(records, time)

    Then("empty duplicates")
    metricStats.collect() should equal(Array())

  }

  it should "one record no duplicates" in {

    Given("unique metric events")

    val eventTime = DateTime.now()

    val events = Seq(
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "B",
        time = eventTime.plusMinutes(2).getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "C",
        time = eventTime.plusMinutes(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[MetricEvent] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[MetricRecordStat] = MetricAnalytic.duplicates(records, time)

    Then("no duplicates")
    metricStats.collect().toSet should equal(Set(
      MetricRecordStat(record = "rec1", metric = "A", day = time.getTime, dup = 0),
      MetricRecordStat(record = "rec1", metric = "B", day = time.getTime, dup = 0),
      MetricRecordStat(record = "rec1", metric = "C", day = time.getTime, dup = 0)
    ))

  }


  it should "one record with duplicates" in {
    Given("repeated metric events in time")

    val eventTime = DateTime.now()

    val events = Seq(
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "B",
        time = eventTime.plusMinutes(2).getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "B",
        time = eventTime.plusMinutes(3).getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "B",
        time = eventTime.plusHours(2).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[MetricEvent] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[MetricRecordStat] = MetricAnalytic.duplicates(records, time)

    Then("duplicates by one record")
    metricStats.collect().toSet should equal(Set(
      MetricRecordStat(record = "rec1", metric = "A", day = time.getTime, dup = 0),
      MetricRecordStat(record = "rec1", metric = "B", day = time.getTime, dup = 1)
    ))
  }

  it should "multiple records no duplicates" in {
    Given("unique metric events for different records")

    val eventTime = DateTime.now()

    val events = Seq(
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      MetricEvent(
        record = "rec2",
        metric = "B",
        time = eventTime.plusMinutes(1).getMillis
      ),
      MetricEvent(
        record = "rec3",
        metric = "A",
        time = eventTime.plusMinutes(2).getMillis
      ),
      MetricEvent(
        record = "rec4",
        metric = "B",
        time = eventTime.plusHours(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[MetricEvent] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[MetricRecordStat] = MetricAnalytic.duplicates(records, time)

    Then("no duplicates")
    metricStats.collect().toSet should equal(Set(
      MetricRecordStat(record = "rec1", metric = "A", day = time.getTime, dup = 0),
      MetricRecordStat(record = "rec2", metric = "B", day = time.getTime, dup = 0),
      MetricRecordStat(record = "rec3", metric = "A", day = time.getTime, dup = 0),
      MetricRecordStat(record = "rec4", metric = "B", day = time.getTime, dup = 0)
    ))
  }

  it should "multiple records with duplicates" in {

    Given("repeated metric events")

    val eventTime = DateTime.now()

    val events = Seq(
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      MetricEvent(
        record = "rec2",
        metric = "B",
        time = eventTime.plusMinutes(1).getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.plusMinutes(2).getMillis
      ),
      MetricEvent(
        record = "rec2",
        metric = "B",
        time = eventTime.plusMinutes(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[MetricEvent] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusHours(1).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[MetricRecordStat] = MetricAnalytic.duplicates(records, time)

    Then("duplicates in both records")
    metricStats.collect().toSet should equal(Set(
      MetricRecordStat(record = "rec1", metric = "A", day = time.getTime, dup = 1),
      MetricRecordStat(record = "rec2", metric = "B", day = time.getTime, dup = 1)
    ))
  }

  it should "duplicates in time range" in {

    val eventTime = DateTime.now()

    Given("range of the same metrics")
    val events = Seq(
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.plusMinutes(1).getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.plusMinutes(2).getMillis
      ),
      MetricEvent(
        record = "rec1",
        metric = "A",
        time = eventTime.plusMinutes(3).getMillis
      )
    )

    When("read records as RDD")

    val records: RDD[MetricEvent] = sc.parallelize(events)

    val time = new Timestamp(eventTime.plusMinutes(2).getMillis)

    When("duplicates calculation")
    val metricStats: RDD[MetricRecordStat] = MetricAnalytic.duplicates(records, time)

    Then("in duplicates are only metrics occurred before calculation time")
    metricStats.collect().toSet should equal(Set(
      MetricRecordStat(record = "rec1", metric = "A", day = time.getTime, dup = 1)
    ))
  }

}