package org.fepipeline.feature.assemble

import org.fepipeline.feature._
import org.fepipeline.util.SchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Assembler.
  *
  * @author Zhang Chaoli
  */
class Assembler extends Transformer with HasInputCols with HasOutputCol {

  def this(inputCols: Array[String], outputCol: String) = {
    this()
    this.inputCols = inputCols
    this.outputCol = outputCol
  }

  def setInputCols(values: Array[String]): Assembler = {
    inputCols = values
    this
  }

  def setInputCols(values: String*): Assembler = {
    inputCols = values.toArray
    this
  }

  def setOutputCol(value: String): Assembler = {
    outputCol = value
    this
  }

  override def transformSchema(schema: StructType): StructType = {
    val fields = schema(inputCols.toSet)
    fields.foreach { fieldSchema =>
      val dataType = fieldSchema.dataType
      val fieldName = fieldSchema.name
      require(dataType.isInstanceOf[NumericType],
        s"Assembler requires columns to be of NumericType. " +
          s"Column $fieldName was $dataType")
    }
    SchemaUtils.appendColumn(schema, StructField(outputCol, LongType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val crossFeature = udf { row: Row =>
      val crossOrg = new StringBuilder()
      var isFirst = true
      var isOver = false
      inputCols.foreach { colName =>
        if (!isOver) {
          val fieldIdx = row.fieldIndex(colName)
          val fValue = row.get(fieldIdx).toString.toLong
          if (fValue < 0) {
            isOver = true
          }
          if (isFirst) {
            crossOrg.append(fValue)
            isFirst = false
          } else {
            crossOrg.append("%06d".format(fValue))
          }
        }
      }
      if (isOver) -1L else crossOrg.toLong
    }

    val metadata = outputSchema(outputCol).metadata
    dataset.select(
      col("*"),
      crossFeature(struct(inputCols.map(col): _*)).as(outputCol, metadata))
  }

}

object Assembler {
  def apply(inputCols: Array[String], outputCol: String): Assembler = new Assembler(inputCols, outputCol)
}
