package org.fepipeline.feature.subsample.dataframe

import org.apache.spark.sql.Row

/**
  * ManyContainOneSubsampler.
  *
  * @author Zhang Chaoli
  */
class ManyContainOneSubsampler(var filterValues: Set[String]) extends PerRecordSubsamplerBase {

  def this() = this(null)

  def this(filterValues: Set[String], values: String*) = {
    this(filterValues)
    this.inputCols = values.toArray
  }

  protected def subsampleFunc: Row => Boolean = (inputs: Row) => {
    filterValues.contains(inputs.get(0).toString)
  }

}

object ManyContainOneSubsampler{
  def apply(slots: Set[String], values: String*): ManyContainOneSubsampler = new ManyContainOneSubsampler(slots, values.toArray: _*)
}
