package storage


import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.rdd.reader.ClassBasedRowReader
import Stats.RecordMetricDuplicates
import org.apache.spark.streaming.Seconds
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, GivenWhenThen, _}
import utils.{CassandraOps, SpecSparkContext}

import scala.collection.JavaConverters._

/**
  * Created by Dmytro Bischak on 4/27/17.
  */
class StatsStorageSpec extends FlatSpec
  with Eventually
  with BeforeAndAfterAll
  with EmbeddedCassandra
  with BeforeAndAfter
  with SpecSparkContext
  with GivenWhenThen
  with Matchers
  with CassandraOps
{

  override def appName = "StatsStorageSpec"

  override def master = "local[2]"

  override def batchDuration = Seconds(2)


  val testKeyspace = "test1"
  val testTable = "table1"
  var conn: CassandraConnector = _

  private val KEYSPACE_FILE = "keyspace.cql"
  private val TABLES_FILE = "tables.cql"
  private val SAMPLE_FILE = "sample2.cql"

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

  "StatsStorage" should "read from Cassandra" in {

    val date24Millis = new DateTime(2017, 4, 24, 0, 0).getMillis
    val date26Millis = new DateTime(2017, 4, 26, 0, 0).getMillis

    val rdd = StatsStorage.readRecordDuplicates()(sc)

    rdd.collect().toSet should equal(
      Set(
        RecordMetricDuplicates(record = "2222", day = date24Millis, metric = "cat1", dup = 5),
        RecordMetricDuplicates(record = "2222", day = date24Millis, metric = "cat2", dup = 10),
        RecordMetricDuplicates(record = "4444", day = date26Millis, metric = "cat1", dup = 300)
      )
    )
  }

  it should "write to Cassandra" in {

    val recordId = UUID.randomUUID().toString

    Given(s"two metric events for record $recordId")

    val dateMillis1 = new DateTime(2017, 4, 27, 0, 0).getMillis
    val dateMillis2 = new DateTime(2017, 4, 25, 0, 0).getMillis

    val duplicates = Seq(
      RecordMetricDuplicates(record = recordId, day = dateMillis1, metric = "cat1", dup = 230),
      RecordMetricDuplicates(record = recordId, day = dateMillis2, metric = "cat2", dup = 4),
      RecordMetricDuplicates(record = recordId, day = dateMillis1, metric = "cat3", dup = 67)
    )

    val rdd = sc.parallelize(duplicates)

    When("write record duplicates in Cassandra")

    StatsStorage.writeRecordDuplicates(rdd)

    val table: TableDef = TableDef.fromType[RecordMetricDuplicates](DDL.KEYSPACE, DDL.RecordMetricDuplicates.TABLE)
    val reader = new ClassBasedRowReader[RecordMetricDuplicates](table, DDL.RecordMetricDuplicates.COLUMNS.map(ColumnName.apply(_, None)))

    conn.withSessionDo {
      session =>

        val resultSet = session.execute(
          s"""SELECT * FROM ${DDL.KEYSPACE}.${DDL.RecordMetricDuplicates.TABLE} WHERE record=?""", recordId
        )

        val metaData = CassandraRowMetadata.fromResultSet(DDL.RecordMetricDuplicates.COLUMNS, resultSet)

        val actual: Set[RecordMetricDuplicates] = resultSet.all()
          .asScala
          .map(reader.read(_, metaData))
          .toSet

        Then("read the same records directly")

        actual should equal(duplicates.toSet)

    }

  }
}
