package org.fepipeline.feature.evaluation

import org.fepipeline.feature.HasInputCol
import org.fepipeline.util.FeatureAnalysisUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

/**
  * InformationEvaluator.
  *
  * @author Zhang Chaoli
  */
class InformationEvaluator(var labelColName: String = "label") extends Evaluator with HasInputCol {

  def this(dataset: Dataset[_], inputCol: String, metricName: String) = {
    this()
    this.inputCol = inputCol
    this.metricName = metricName
    this.dataset = dataset
  }

  def this(dataset: Dataset[_], labelColName: String, inputCol: String, metricName: String) = {
    this(labelColName)
    this.inputCol = inputCol
    this.metricName = metricName
    this.dataset = dataset
  }

  def setDataset(dataset: Dataset[_]): InformationEvaluator = {
    this.dataset = dataset
    this
  }

  def setLabelColName(value: String): InformationEvaluator = {
    labelColName = value
    this
  }

  def setInputCol(inputCol: String): InformationEvaluator = {
    this.inputCol = inputCol
    this
  }

  private lazy val sampleStatList = {
    val allColName = labelColName + "_all"
    val posColName = labelColName + "_pos"
    dataset.groupBy(inputCol)
      .agg(count(labelColName).as(allColName), sum(labelColName).as(posColName))
      .select(inputCol, allColName, posColName)
      .collect()
      .map { row =>
        val inputColValue = row.get(row.fieldIndex(inputCol)).toString
        val allNum = row.getAs[Long](allColName)
        val positiveNum = row.getAs[Long](posColName)
        new SampleStatEntity[String](inputColValue, allNum, positiveNum, allNum - positiveNum)
      }.toList
  }

  private lazy val woe: Double = {
    0.0
  }

  private lazy val infoGain: Double = FeatureAnalysisUtils.getInfoGain(sampleStatList.asJava)

  def evaluate: Double = {
    val metric = metricName match {
      case "infoGain" => infoGain
      case "infoGainRatio" => getInfoGainRatio(dataset)
      case "infoValue" => getInfoValue(dataset)
      case "mutualInformation" => getMutualInformation(dataset)
      case _ => throw new IllegalArgumentException("Metric name is mismatch.")
    }
    metric
  }

  def getInfoGainRatio(dataset: Dataset[_]): Double = FeatureAnalysisUtils.getInfoGainRatio(sampleStatList.asJava, infoGain)

  def getInfoValue(dataset: Dataset[_]): Double = {
    0.0
  }

  def getMutualInformation(dataset: Dataset[_]): Double = {
    0.0
  }

}

object InformationEvaluator {
  def apply(dataset: Dataset[_], inputCol: String, metricName: String): InformationEvaluator = new InformationEvaluator(dataset, inputCol, metricName)

  def apply(dataset: Dataset[_], labelColName: String, inputCol: String, metricName: String): InformationEvaluator = new InformationEvaluator(dataset, labelColName, inputCol, metricName)
}
