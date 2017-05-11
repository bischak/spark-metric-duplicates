package storage

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import storage.Events.Record
import storage.Stats.{MetricDuplicates, RecordMetricDuplicates}

/**
  * Created by Dmytro Bischak on 5/3/17.
  */

object DDL {

  val KEYSPACE = "metrics"

  object Records {
    val TABLE = "records"
    val COLUMNS = IndexedSeq("record", "time", "metric")
  }

  object RecordMetricDuplicates {
    val TABLE = "records_duplicates"
    val COLUMNS = IndexedSeq("record", "metric", "day", "dup")
  }

  object MetricDuplicates {
    val TABLE = "duplicates"
  }


}

object MetricStorage {

  def read(record: String)(implicit sparkContext: SparkContext): RDD[Record] = {
    sparkContext
      .cassandraTable[Record](keyspace = DDL.KEYSPACE, DDL.Records.TABLE)
      .where("record = ?", record)
  }

  def all()(implicit sparkContext: SparkContext): RDD[Record] = {
    sparkContext
      .cassandraTable[Record](keyspace = DDL.KEYSPACE, DDL.Records.TABLE)
  }

  def write(metrics: RDD[Record]): Unit = {
    metrics.saveToCassandra(DDL.KEYSPACE, DDL.Records.TABLE, AllColumns)
  }

}

object StatsStorage {

  def writeRecordDuplicates(metrics: RDD[RecordMetricDuplicates]): Unit = {
    metrics.saveToCassandra(DDL.KEYSPACE, DDL.RecordMetricDuplicates.TABLE, AllColumns)
  }

  def readRecordDuplicates()(implicit sparkContext: SparkContext): RDD[RecordMetricDuplicates] = {
    sparkContext
      .cassandraTable[RecordMetricDuplicates](keyspace = DDL.KEYSPACE, DDL.RecordMetricDuplicates.TABLE)
  }

  def writeMetricDuplicates(metrics: RDD[MetricDuplicates]): Unit = {
    metrics.saveToCassandra(DDL.KEYSPACE, DDL.MetricDuplicates.TABLE, AllColumns)
  }

  def readMetricDuplicates(day: Long)(implicit sparkContext: SparkContext): RDD[MetricDuplicates] = {
    sparkContext
      .cassandraTable[MetricDuplicates](keyspace = DDL.KEYSPACE, DDL.MetricDuplicates.TABLE)
      .where("day = ?", day)
  }


}