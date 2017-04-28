package utils

import java.nio.file.Files

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

/**
  * Created by Dmytro Bischak on 4/27/17.
  */
trait SpecSparkContext {

  this: BeforeAndAfter =>

  var sc: SparkContext = _
  var ssc: StreamingContext = _

  def appName: String

  def master: String

  def batchDuration: Duration

  before {

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)

    val context = new StreamingContext(conf, batchDuration)
    context.checkpoint(Files.createTempDirectory(appName).toString)
    ssc = context
    sc = context.sparkContext

  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

}
