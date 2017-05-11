package utils

import com.datastax.driver.core.ResultSet

import scala.io.Source

/**
  * Created by Dmytro Bischak on 5/10/17.
  */
trait CassandraOps {

  def cqlStatements(cqlFile: String): Array[String] = {

    Source.fromFile(getClass.getClassLoader.getResource(cqlFile).toURI)
      .mkString.trim.split(';')
  }

  def executeFiles(execute: String => ResultSet, cqlFile: String*): Unit = cqlFile.foreach {
    file => cqlStatements(file).foreach(execute)
  }

}
