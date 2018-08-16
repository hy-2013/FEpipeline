package org.fepipeline.feature.evaluation

import org.fepipeline.common.Logging
import org.apache.spark.sql.Dataset

/**
  * Evaluator.
  *
  * @author Zhang Chaoli
  */
trait Evaluator extends Logging with Serializable {

  var metricName: String = _

  var dataset: Dataset[_] = _

  def getMetricName: String = metricName

  def setMetricName(value: String): this.type = {
    metricName = value
    this
  }

//  def isLargerBetter: Boolean = true

  def evaluate: Double

}
