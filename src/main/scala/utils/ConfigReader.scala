package utils

import org.apache.spark.SparkConf
import scala.io.{BufferedSource, Source}

object ConfigReader {

  /**
   * Transform property file to SparkConf
   *
   * @param path path to property file
   * @return SparkConf with map of  fs.azure.account.*  properties
   */
  def getAuthConfig(path: String): SparkConf = {
    val configFile: BufferedSource = Source.fromFile(path)
    try {
      new SparkConf().setAll(toKeyValueConfigIterator(configFile).toIterable)
    } finally {
      configFile.close()
    }
  }

  /**
   * Transform property file to Map
   * @param path path to property file
   * @return Map of required Azure properties
   */
  def getConfig(path: String): Map[String, String] = {
    val configFile = Source.fromFile(path)
    try {
      toKeyValueConfigIterator(configFile).toMap
    } finally {
      configFile.close()
    }
  }

  /**
   * Map source file to an iterator of key-value property pairs
   * @param config block of properties to be converted to a map.
   * @return iterator by key-values from source file
   */
  private def toKeyValueConfigIterator(config: BufferedSource) = {
    config.getLines.map {
      line =>
        val split = line.split("=")
        split(0) -> split(1)
    }
  }
}
