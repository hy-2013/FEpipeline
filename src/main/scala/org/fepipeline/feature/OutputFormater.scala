package org.fepipeline.feature

import org.fepipeline.feature.conversion.Location
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * OutputFormater.
  *
  * @author Zhang Chaoli
  */
abstract class OutputFormater extends Transformer with HasOutputCol {

}

class FtrlOutputFormater(var colNameIndexes: Array[(String, Int)]) extends OutputFormater with HasInputCols {

  def this() = this(null)

  /**
    * @param colNameIndexes
    * @param outputCol
    * @param headElems the order shoud be imei\001cookieid\001userid\001platform\001ptype\001slot\001infoid\001sid\tlabel
    */
  def this(colNameIndexes: Array[(String, Int)], outputCol: String, headElems: Array[String] = Array("imei", "cookieid", "userid", "platform", "ptype", "slot", "infoid", "sid", "label")) = {
    this(colNameIndexes)
    this.inputCols = headElems
    this.outputCol = outputCol
  }

  def setInputCols(headElems: Array[String]): FtrlOutputFormater = {
    inputCols = headElems
    this
  }

  def setInputCols(headElems: String*): FtrlOutputFormater = {
    inputCols = headElems.toArray
    this
  }

  def setOutputCol(value: String): FtrlOutputFormater = {
    outputCol = value
    this
  }

  def setColNameIndexes(colName2Setids: Array[(String, Int)]): FtrlOutputFormater = {
    this.colNameIndexes = colName2Setids
    this
  }

  def transformSchema(schema: StructType): StructType = {
    require(inputCols.length == 9, s"All head elements should be imei\001cookieid\001userid\001platform\001ptype\001slot\001infoid\001sid\tlabel" +
      s"and the array length should be equal to 9.")
    StructType(Array(StructField(outputCol, StringType, false)))
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val sampleUDF = udf { row: Row =>
      val sampleHeader = mutable.ArrayBuffer[String]()
      inputCols.foreach { colName =>
        val fieldIdx = row.fieldIndex(colName)
        val headElem = row.get(fieldIdx).toString
        sampleHeader += headElem
      }

      val setidValueids = mutable.ArrayBuffer[String]()
      colNameIndexes.foreach { colName2Setid =>
        val colName = colName2Setid._1
        val setid = colName2Setid._2
        val fieldIdx = row.fieldIndex(colName)
        try {
          val valueid = row.get(fieldIdx).toString.toLong
          setidValueids += setid + ":" + valueid
        } catch {
          case e: Exception =>
            require(false, "FtrlOutputFormater-transform valueid should be Long type Exception.")
        }
      }

      sampleHeader.dropRight(1).mkString("\001") + "\t" + sampleHeader.last + "\t" + setidValueids.mkString("\t")
    }

    val metadata = outputSchema(outputCol).metadata
    dataset.select(
      sampleUDF(struct(col("*"))).as(outputCol, metadata))
  }
}

object FtrlOutputFormater {
  def apply(colNameIndexes: Array[(String, Int)], outputCol: String, headElems: Array[String]): FtrlOutputFormater = new FtrlOutputFormater(colNameIndexes, outputCol, headElems)

  def apply(colName2Setids: Array[(String, Int)], outputCol: String): FtrlOutputFormater = new FtrlOutputFormater(colName2Setids, outputCol)
}


class IdTableOutputFormater(idColName: String, tableName: String, batch: Int, date: String) extends OutputFormater with HasInputCols {
  def transformSchema(schema: StructType): StructType = null

  def transform(dataset: Dataset[_]): DataFrame = {
    val paramsUDF = udf { row: Row =>
      val params = mutable.HashMap[String, String]()
      inputCols.foreach { colName =>
        val fieldIdx = row.fieldIndex(colName)
        val headElem = row.get(fieldIdx).toString
        params += (colName -> headElem)
      }
      params
    }

    val metadata = StructField("params", MapType(StringType, StringType), false).metadata
    val tableDataset = dataset.select(
      col(idColName),
      paramsUDF(struct(col("*"))).as("params", metadata))

    tableDataset.createOrReplaceTempView("idTable")
    dataset.sparkSession.sql(s"insert overwrite table hdp_lbg_supin_zplisting.$tableName partition(dt='$date',batch=$batch) select $idColName, params from idTable")
  }
}

object IdTableOutputFormater {
  def apply(idColName: String, tableName: String, batch: Int, date: String): IdTableOutputFormater = new IdTableOutputFormater(idColName, tableName, batch, date)
}


class FtrlFromFtrlOutputFormater(outputDataType: DataType) extends MultifoldTransformer[String, FtrlFromFtrlOutputFormater](outputDataType) {

  def this(outputCol: String, delSource: Boolean = true, inputCols: Array[String] = Array("imei", "cookieid", "userid", "platform", "ptype", "slot", "infoid", "sid", "label", "features")) = {
    this(StringType)
    this.outputCol = outputCol
    this.inputCols = inputCols
    this.delSource = delSource
  }

  override protected def createTransformFunc: Row => String = (inputs: Row) => {
    val (imei, cookie, userId, platform, pageType, slot, infoId, sid, label, features) = (inputs.getString(0), inputs.getString(1), inputs.getString(2), inputs.getString(3), inputs.getString(4), inputs.getString(5), inputs.getString(6), inputs.getString(7), inputs.getString(8), inputs.getString(9))

    imei + "\1" + cookie + "\1" + userId + "\1" + platform + "\1" + pageType + "\1" + slot + "\1" + infoId + "\1" + sid + "\t" + label + "\t" + features
  }

}

object FtrlFromFtrlOutputFormater {
  def apply(outputCol: String): FtrlFromFtrlOutputFormater = new FtrlFromFtrlOutputFormater(outputCol)
}


class DropColumnsOutputFormater extends OutputFormater with HasInputCols {

  def this(inputCols: String*) {
    this()
    this.inputCols = inputCols.toArray
  }

  def transformSchema(schema: StructType): StructType = null

  def transform(dataset: Dataset[_]): DataFrame = {
    dataset.drop(inputCols: _*)
  }
}

object DropColumnsOutputFormater {
  def apply(inputCols: String*): DropColumnsOutputFormater = new DropColumnsOutputFormater(inputCols.toArray: _*)
}