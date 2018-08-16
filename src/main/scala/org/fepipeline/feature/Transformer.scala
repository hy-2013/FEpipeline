package org.fepipeline.feature

import org.fepipeline.common.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * Transformer.
  *
  * @author Zhang Chaoli
  */
abstract class Transformer extends PipelineStage {
  def transform(dataset: Dataset[_]): DataFrame

}

abstract class UnaryTransformer[IN, OUT, T <: UnaryTransformer[IN, OUT, T]]
(outputDataType: DataType) extends Transformer with HasInputCol with HasOutputCol with Logging {

  def setInputCol(value: String): T = {
    inputCol = value
    this.asInstanceOf[T]
  }

  def setOutputCol(value: String): T = {
    outputCol = value
    this.asInstanceOf[T]
  }

  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: IN => OUT

  /**
    * Validates the input type. Throw an exception if it is invalid.
    */
  protected def validateInputType(inputType: DataType): Unit = {}

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(inputCol).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains(outputCol)) {
      throw new IllegalArgumentException(s"Output column $outputCol already exists.")
    }
    val outputFields = schema.fields :+
      StructField(outputCol, outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(this.createTransformFunc, outputDataType)
    dataset.withColumn(outputCol, transformUDF(dataset(inputCol)))
  }

}

abstract class MultifoldTransformer[OUT, T <: MultifoldTransformer[OUT, T]]
(outputDataType: DataType, var delSource: Boolean = false) extends Transformer with HasInputCols with HasOutputCol with Logging {

  def setInputCols(values: Array[String]): T = {
    inputCols = values
    this.asInstanceOf[T]
  }

  def setInputCols(values: String*): T = {
    inputCols = values.toArray
    this.asInstanceOf[T]
  }

  def setOutputCol(value: String): T = {
    outputCol = value
    this.asInstanceOf[T]
  }

  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: Row => OUT

  /**
    * Validates the input type. Throw an exception if it is invalid.
    */
  protected def validateInputType(inputType: StructType): Unit = {}

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema(inputCols.toSet)
    validateInputType(inputType)
    if (schema.fieldNames.contains(outputCol)) {
      throw new IllegalArgumentException(s"Output column $outputCol already exists.")
    }
    val outputFields = if (delSource) Array(StructField(outputCol, outputDataType, nullable = false))
    else schema.fields :+ StructField(outputCol, outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(this.createTransformFunc, outputDataType)

    val metadata = outputSchema(outputCol).metadata
    if (delSource) dataset.select(transformUDF(struct(inputCols.map(col): _*)).as(outputCol, metadata))
    else dataset.withColumn(outputCol, transformUDF(struct(inputCols.map(col): _*)))
  }

}