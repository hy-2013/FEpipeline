package org.fepipeline.feature.conversion

import org.fepipeline.feature._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes}

/**
  * Convertor.
  *
  * @author Zhang Chaoli
  */
class TitleLenConvertor(var outputDataType: DataType) extends UnaryTransformer[String, Int, TitleLenConvertor](outputDataType) {

  def this(inputCol: String, outputCol: String, outputDataType: DataType = DataTypes.IntegerType) = {
    this(outputDataType)
    this.inputCol = inputCol
    this.outputCol = outputCol
  }

  def this() = this(null, null)

  override protected def createTransformFunc: String => Int = (input: String) => input.length
}

object TitleLenConvertor {
  def apply(inputCol: String, outputCol: String): TitleLenConvertor = new TitleLenConvertor(inputCol, outputCol)
}


class WelCntConvertor(outputDataType: DataType) extends UnaryTransformer[String, Int, TitleLenConvertor](outputDataType) {

  def this(inputCol: String, outputCol: String, outputDataType: DataType = DataTypes.IntegerType) = {
    this(outputDataType)
    this.inputCol = inputCol
    this.outputCol = outputCol
  }

  def this() = this(null, null)

  override protected def createTransformFunc: String => Int = (input: String) => input.split(",").length

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == DataTypes.StringType, s"Bad input type: $inputType. Requires Double.")
  }
}

object WelCntConvertor {
  def apply(inputCol: String, outputCol: String): WelCntConvertor = new WelCntConvertor(inputCol, outputCol)
}


case class Location(lat: Double, lon: Double)

class LonLatDistanceConvertor(outputDataType: DataType) extends MultifoldTransformer[Long, LonLatDistanceConvertor](outputDataType) {

  def this(inputCols: Array[String], outputCol: String, outputDataType: DataType = DataTypes.LongType) = {
    this(outputDataType)
    this.inputCols = inputCols
    this.outputCol = outputCol
  }

  def this() = this(null, null)

  override protected def createTransformFunc: Row => Long = (inputs: Row) => {
    val (userLat, userLon, infoLat, infoLon) = (inputs.get(0).toString, inputs.get(1).toString, inputs.get(2).toString, inputs.get(3).toString)

    if (userLat.length < 3 || userLon.length < 3 || infoLat.length < 3 || infoLon.length < 3) {
      -1L
    } else {
      try {
        calculateDistanceInMeter(Location(userLat.toDouble, userLon.toDouble), Location(infoLat.toDouble, infoLon.toDouble))
      } catch {
        case t: Throwable =>
          logInfo("Calculate latlon Distance Exception.")
          -1L
      }
    }
  }

  /**
    * Haversine formula calculate circle distance
    *
    * @param userLocation
    * @param infoLocation
    * @return
    */
  def calculateDistanceInMeter(userLocation: Location, infoLocation: Location): Long = {
    val p = 0.017453292519943295 // Math.PI / 180

    var a = 0.5 - Math.cos((userLocation.lat - infoLocation.lat) * p) / 2 +
      Math.cos(infoLocation.lat * p) * Math.cos(userLocation.lat * p) *
        (1 - Math.cos((userLocation.lon - infoLocation.lon) * p)) / 2

    (12742000 * Math.asin(Math.sqrt(a))).toLong // 2 * R; R = 6371 * 1000 m
  }

}

object LonLatDistanceConvertor {
  def apply(inputCols: Array[String], outputCol: String): LonLatDistanceConvertor = new LonLatDistanceConvertor(inputCols, outputCol)
}
