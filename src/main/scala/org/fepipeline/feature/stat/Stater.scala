package org.fepipeline.feature.stat

import org.fepipeline.feature.{HasInputCol, HasInputCols, HasOutputCol, Transformer}
import org.fepipeline.util.SchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Stater.
  *
  * @author Zhang Chaoli
  */
abstract class Stater extends Transformer with HasInputCol with HasInputCols with HasOutputCol {

}

class Aggregater extends Stater {

  def this(inputCol: String, outputCol: String, inputCols: Array[String]) = {
    this()
    this.inputCol = inputCol
    this.outputCol = outputCol
    this.inputCols = inputCols
  }

  def this(inputCol: String, outputCol: String) = this(inputCol, outputCol, null)

  def this(inputCols: Array[String], outputCol: String) = this(null, outputCol, inputCols)

  def setInputCol(value: String): Aggregater = {
    inputCol = value
    this
  }

  def setInputCols(values: Array[String]): Aggregater = {
    inputCols = values
    this
  }

  def setInputCols(values: String*): Aggregater = {
    inputCols = values.toArray
    this
  }

  def setOutputCol(value: String): Aggregater = {
    outputCol = value
    this
  }

  def transformSchema(schema: StructType): StructType = {
    SchemaUtils.appendColumn(schema, StructField(outputCol, LongType, true))
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema(outputCol).metadata
    val inputColStat = dataset.groupBy(inputCol).agg(count(inputCol).as(outputCol, metadata))
    dataset.join(inputColStat, Seq(inputCol), "leftouter")
  }

}

object Aggregater {
  def apply(inputCol: String, outputCol: String, inputCols: Array[String]): Aggregater = new Aggregater(inputCol, outputCol, inputCols)
  def apply(inputCol: String, outputCol: String): Aggregater = new Aggregater(inputCol, outputCol, null)
  def apply(inputCols: Array[String], outputCol: String): Aggregater = new Aggregater(null, outputCol, inputCols)
}

class CXRStater(var labelColName: String) extends Stater {

  def this(labelColName: String, inputCol: String, outputCol: String, inputCols: Array[String]) = {
    this(labelColName)
    this.inputCol = inputCol
    this.outputCol = outputCol
    this.inputCols = inputCols
  }

  def this(inputCol: String, outputCol: String) {
    this("click", inputCol, outputCol, null)
  }

  def this(labelColName: String, inputCol: String, outputCol: String) {
    this(labelColName, inputCol, outputCol, null)
  }

  def this(labelColName: String, inputCols: Array[String], outputCol: String) {
    this(labelColName, null, outputCol, inputCols)
  }

  def this() = this(null)

  def setInputCol(value: String): CXRStater = {
    inputCol = value
    this
  }

  def setInputCols(values: Array[String]): CXRStater = {
    inputCols = values
    this
  }

  def setInputCols(values: String*): CXRStater = {
    inputCols = values.toArray
    this
  }

  def setOutputCol(value: String): CXRStater = {
    outputCol = value
    this
  }

  def setLabelColName(value: String): CXRStater = {
    labelColName = value
    this
  }

  def getLabelColName = labelColName

  def transformSchema(schema: StructType): StructType = {
    val labelDataType = schema(labelColName).dataType
    require(labelDataType.isInstanceOf[NumericType], s"Assembler requires columns to be of NumericType. " +
      s"Column $labelColName was $labelDataType")
    SchemaUtils.appendColumn(schema, StructField(outputCol, DoubleType, true))
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val sumColName = labelColName + "_sum"
    val cntColName = labelColName + "_cnt"
    val cxrFunc = udf { row: Row =>
      val sum = row.getAs[Long](sumColName)
      val cnt = row.getAs[Long](cntColName)
      if (cnt > 0) {
        sum.toDouble / cnt
      } else 0.0
    }

    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema(outputCol).metadata
    val inputColStat = dataset.groupBy(inputCol)
      .agg(sum(labelColName).as(sumColName), count(labelColName).as(cntColName))
      .select(col(inputCol), cxrFunc(struct(col(sumColName), col(cntColName))).as(outputCol, metadata))

    dataset.join(inputColStat, Seq(inputCol), "leftouter")
  }

}

object CXRStater {
  def apply(labelColName: String, inputCol: String, outputCol: String, inputCols: Array[String]): CXRStater = new CXRStater(labelColName, inputCol, outputCol, inputCols)
  def apply(labelColName: String, inputCol: String, outputCol: String): CXRStater = new CXRStater(labelColName, inputCol, outputCol, null)
}

class BayesianSmoothing extends Stater {
  def transformSchema(schema: StructType): StructType = null
  def transform(dataset: Dataset[_]): DataFrame = null
}
