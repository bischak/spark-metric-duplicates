package transformers

import storage.Events.Record
import org.apache.spark.streaming.Seconds
import org.scalatest._
import utils.SpecSparkContext

/**
  * Created by Dmytro Bischak on 4/26/17.
  */

class EventsTransformerSpec extends FlatSpec with BeforeAndAfter with SpecSparkContext with GivenWhenThen with Matchers {

  override def appName = "CollectorSpec"

  override def master = "local[2]"

  override def batchDuration = Seconds(2)

  "EventsTransformer" should "process JSON lines" in {

    Given("two events in Json format")
    val events = Seq(
      """{"record":"e55c31ae-b965-41a7-a4c3-b8c4178dedd3","metric":"CATEGORY2","time":1493138491461}""",
      """{"record":"e55c31ae-b965-41a7-a4c3-b8c4178dedd3","metric":"CATEGORY2","time":1493136538770}"""
    )

    When("read from Kafka as RDD")
    val eventsRDD = sc.parallelize(events)

    Then("transform to MetricEvent RDD")
    EventsTransformer.processFromJson(eventsRDD).collect() should equal(Array(
      Record(record = "e55c31ae-b965-41a7-a4c3-b8c4178dedd3", metric = "CATEGORY2", time = 1493138491461L),
      Record(record = "e55c31ae-b965-41a7-a4c3-b8c4178dedd3", metric = "CATEGORY2", time = 1493136538770L)
    ))

  }


}