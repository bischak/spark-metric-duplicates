package transformers

import storage.Events.Record
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Dmytro Bischak on 4/26/17.
  */
object EventsTransformer {

  def processFromJson(events: RDD[String]): RDD[Record] = {

    val session = SparkSession.builder().getOrCreate()

    import session.implicits._

    session.read.json(events).as[Record].rdd
  }

}