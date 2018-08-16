package org.fepipeline.feature.subsample.dataframe

import org.apache.spark.sql.Row

/**
  * OneEqualOneSubsampler.
  *
  * @author Zhang Chaoli
  */
class OneEqualOneSubsampler(var filterValue: String) extends PerRecordSubsamplerBase {

  def this() = this(null)

  def this(filterValue: String, values: String*) = {
    this(filterValue)
    this.inputCols = values.toArray
  }

  def setFilterValue(value: String): OneEqualOneSubsampler = {
    this.filterValue = value
    this
  }

  protected def subsampleFunc: Row => Boolean = (inputs: Row) => {
    filterValue.equals(inputs.get(0).toString)
  }
}

object OneEqualOneSubsampler {
  def apply(filterValue: String, values: String*): OneEqualOneSubsampler = new OneEqualOneSubsampler(filterValue, values.toArray: _*)
}
