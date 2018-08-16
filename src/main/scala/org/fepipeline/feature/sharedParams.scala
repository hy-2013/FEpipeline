package org.fepipeline.feature

/**
  * HasInputCol.
  *
  * @author Zhang Chaoli
  */
trait HasInputCol {

  /**
    * Param for input column name.
    */
  var inputCol: String = null

  def getInputCol: String = inputCol
}

trait HasInputCols {

  /**
    * Param for input column names.
    */
  var inputCols: Array[String] = Array.empty

  def getInputCols: Array[String] = inputCols
}

trait HasOutputCol {

  /**
    * Param for output column name.
    */
  var outputCol: String = null

  def getOutputCol: String = outputCol
}

trait HasOutputCols {

  /**
    * Param for output column names.
    */
  var outputCols: Array[String] = Array.empty

  def getOutputCols: Array[String] = outputCols
}

