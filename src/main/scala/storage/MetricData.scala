package storage

/**
  * Created by Dmytro Bischak on 4/24/17.
  */

object Events {

  case class Record(record: String, time: Long, metric: String)

}


object Stats {

  case class RecordMetricDuplicates(record: String, metric: String, day: Long, dup: Int)

  case class MetricDuplicates(metric: String, day: Long, dup: Int)

}


