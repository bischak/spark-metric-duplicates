package storage


import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReader
import Events.Record
import org.apache.spark.streaming.Seconds
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, GivenWhenThen, _}
import utils.{CassandraOps, SpecSparkContext}

import scala.collection.JavaConverters._

/**
  * Created by Dmytro Bischak on 4/27/17.
  */
class MetricStorageSpec extends FlatSpec
  with Eventually
  with BeforeAndAfterAll
  with EmbeddedCassandra
  with BeforeAndAfter
  with SpecSparkContext
  with GivenWhenThen
  with Matchers
  with CassandraOps
{

  override def appName = "MetricStorageSpec"

  override def master = "local[2]"

  override def batchDuration = Seconds(2)


  val testKeyspace = "test1"
  val testTable = "table1"
  var conn: CassandraConnector = _

  private val KEYSPACE_FILE = "keyspace.cql"
  private val TABLES_FILE = "tables.cql"
  private val SAMPLE_FILE = "sample1.cql"


  override def beforeAll(): Unit = {
    conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
    conn.withSessionDo { session =>

      info("Creating Cassandra Schema")

      executeFiles(session.execute, KEYSPACE_FILE, TABLES_FILE, SAMPLE_FILE)
    }

  }


  override protected def afterAll(): Unit = {
    super.afterAll()
    CassandraConnector.evictCache()
  }

  override def clearCache: Unit = Unit

  "MetricStorage" should "read from Cassandra" in {

    val dataRDD = MetricStorage.read(record = "2222")(sc)

    dataRDD.collect().toSet should equal(
      Set(
        Record(record = "2222", time = new DateTime(2017, 4, 24, 12, 35).getMillis, metric = "cat1"),
        Record(record = "2222", time = new DateTime(2017, 4, 24, 12, 35).getMillis, metric = "cat2"),
        Record(record = "2222", time = new DateTime(2017, 4, 24, 12, 40).getMillis, metric = "cat1"),
        Record(record = "2222", time = new DateTime(2017, 4, 24, 12, 50).getMillis, metric = "cat1")
      )
    )

  }

  it should "write to Cassandra" in {

    val recordId = UUID.randomUUID().toString

    Given(s"two metric events for record $recordId")

    val metrics = Seq(
      Record(record = recordId, time = new DateTime(2017, 4, 24, 14, 30).getMillis, metric = "cat3"),
      Record(record = recordId, time = new DateTime(2017, 4, 24, 14, 37).getMillis, metric = "cat4")
    )

    val rdd = sc.parallelize(metrics)

    When("write records in Cassandra")

    MetricStorage.write(rdd)

    val table: TableDef = TableDef.fromType[Record](DDL.KEYSPACE, DDL.Records.TABLE)

    val reader = new ClassBasedRowReader[Record](table, DDL.Records.COLUMNS.map(ColumnName.apply(_, None)))

    conn.withSessionDo {
      session =>

        val resultSet = session.execute(
          s"""SELECT * FROM ${DDL.KEYSPACE}.${DDL.Records.TABLE} WHERE record = ?""", recordId
        )

        val metadata = CassandraRowMetadata.fromResultSet(DDL.Records.COLUMNS, resultSet)

        val actual: Set[Record] = resultSet.all()
          .asScala
          .map(reader.read(_, metadata))
          .toSet

        Then("read the same records directly")

        actual should equal(metrics.toSet)

    }

  }
}
