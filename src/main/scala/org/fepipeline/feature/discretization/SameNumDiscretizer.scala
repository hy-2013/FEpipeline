package org.fepipeline.feature.discretization

import org.fepipeline._
import org.fepipeline.util.DiscretizationUtil
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
  * Discretizer and DiscretizerModel.
  *
  * @author Zhang Chaoli
  */

class SameNumDiscretizer(var boundNum: Int) extends Discretizer[BoundsDiscretizerModel] {

  def this() = this(10)

  def this(inputCol: String, outputCol: String, boundNum: Int = 10) = {
    this(boundNum)
    this.inputCol = inputCol
    this.outputCol = outputCol
  }

  def setBoundNum(value: Int): SameNumDiscretizer = {
    boundNum = value
    this
  }

  def setInputCol(value: String): SameNumDiscretizer = {
    inputCol = value
    this
  }

  def setOutputCol(value: String): SameNumDiscretizer = {
    outputCol = value
    this
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColDataType = schema(inputCol).dataType
    validateAndTransformSchema(schema, inputColDataType)
  }

  override def fit(dataset: Dataset[_]): BoundsDiscretizerModel = {
    val numValueArr = dataset.groupBy(inputCol).agg(count(inputCol)).rdd
      .map(row => (row.get(1).toString.toLong, row.getAs(0).toString.toDouble))
      .collect().sortBy(_._2)
    val bounds = (new DiscretizationUtil).getBounds(inputCol, numValueArr, boundNum = boundNum)

    new BoundsDiscretizerModel(inputCol, outputCol, bounds)
  }
}

object SameNumDiscretizer {
  def apply(inputCol: String, outputCol: String): SameNumDiscretizer = new SameNumDiscretizer(inputCol, outputCol)

  def apply(inputCol: String, outputCol: String, boundNum: Int): SameNumDiscretizer = new SameNumDiscretizer(inputCol, outputCol, boundNum)
}

class BoundsDiscretizerModel(bounds: Array[Double]) extends DiscretizerModel[BoundsDiscretizerModel] {

  def this(inputCol: String, outputCol: String, bounds: Array[Double]) = {
    this(bounds)
    this.inputCol = inputCol
    this.outputCol = outputCol
  }

  def getBounds: Array[Double] = bounds

  override def transformSchema(schema: StructType): StructType = {
    val inputColDataType = schema(inputCol).dataType
    validateAndTransformSchema(schema, inputColDataType)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val discUDF = udf { row: Row =>
      val value = row.get(row.fieldIndex(inputCol)).toString.toDouble
      DiscretizationUtil.boundsDiscretize(value, ArrayUtils.toObject(bounds).toList.asJava)
    }

    dataset.withColumn(outputCol, discUDF(struct(col(inputCol))))
  }
}

object BoundsDiscretizerModel {
  def apply(inputCol: String, outputCol: String, bounds: Array[Double]): BoundsDiscretizerModel = new BoundsDiscretizerModel(inputCol, outputCol, bounds)
}
