package metrics

import java.sql.Date

/**
  * Created by Dmytro Bischak on 4/24/17.
  */

case class MetricRecord(id: String, date: Date)

case class MetricEvent(record: String, time: Long, metric: String)

case class MetricRecordStat(record: String, metric: String, day: Long, dup: Int)
