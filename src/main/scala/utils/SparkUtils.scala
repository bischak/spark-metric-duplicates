package utils

import com.typesafe.config.Config
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._


/**
  * Created by Dmytro Bischak on 4/24/17.
  */
object SparkUtils {

  def toSparkConfig(config: Config): SparkConf = {
    val sparkConf = new SparkConf()
    for {es <- config.getConfig("spark").entrySet().asScala} {
      es.getValue.unwrapped() match {
        case value: String =>
          sparkConf.set(s"spark.${es.getKey}", value)
        case _ =>
      }
    }
    sparkConf
  }

}
