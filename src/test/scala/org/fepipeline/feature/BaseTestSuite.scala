package org.fepipeline.feature

import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class BaseTestSuite extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var sc: SparkContext = _
  var rawSample: DataFrame = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("[Huye] BaseTestSuite")
      .enableHiveSupport()
      .getOrCreate()

    sc = spark.sparkContext
    rawSample = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(classOf[BaseTestSuite].getClassLoader.getResource("people.csv").toString)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }
}
