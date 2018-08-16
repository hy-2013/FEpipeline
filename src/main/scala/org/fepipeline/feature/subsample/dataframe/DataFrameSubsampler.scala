package org.fepipeline.feature.subsample.dataframe

import org.fepipeline.feature.HasInputCols
import org.fepipeline.feature.subsample.Subsampler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Row, _}

/**
  * DataFrameSubsampler.
  *
  * @author Zhang Chaoli
  */
abstract class DataFrameSubsampler extends Subsampler with HasInputCols {

  def setInputCols(values: Array[String]): Subsampler = {
    inputCols = values
    this
  }

  def setInputCols(values: String*): Subsampler = {
    inputCols = values.toArray
    this
  }

}

abstract class PerRecordSubsamplerBase extends DataFrameSubsampler {

  protected def subsampleFunc: Row => Boolean

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val subsampleUDF = udf(this.subsampleFunc, BooleanType)
    dataset.filter(subsampleUDF(struct(inputCols.map(col): _*))).toDF()
  }

}
