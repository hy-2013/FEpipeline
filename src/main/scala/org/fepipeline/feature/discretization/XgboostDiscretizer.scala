package org.fepipeline.feature.discretization

import org.fepipeline.feature.Model
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

/**
  * XgboostDiscretizer.
  *
  * @author Zhang Chaoli
  */
class XgboostDiscretizer extends Discretizer[XgboostDiscretizerModel] {

  def this(inputCol: String, outputCol: String) = {
    this()
    this.inputCol = inputCol
    this.outputCol = outputCol
  }

  def setInputCol(value: String): XgboostDiscretizer = {
    inputCol = value
    this
  }

  def setOutputCol(value: String): XgboostDiscretizer = {
    outputCol = value
    this
  }

  override def transformSchema(schema: StructType): StructType = null

  override def fit(dataset: Dataset[_]): XgboostDiscretizerModel = {
    null
  }
}

class XgboostDiscretizerModel extends DiscretizerModel[XgboostDiscretizerModel] {

  override def transformSchema(schema: StructType): StructType = null

  override def transform(dataset: Dataset[_]): DataFrame = null
}