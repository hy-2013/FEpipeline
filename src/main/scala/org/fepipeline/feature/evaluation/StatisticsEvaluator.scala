package org.fepipeline.feature.evaluation

import org.fepipeline.feature.{HasInputCol, HasInputCols}
import org.fepipeline.util.SchemaUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.stat.StatFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Statistical Evaluator.
  *
  * @author Zhang Chaoli
  */
class StatisticsEvaluator(var labelColName: String = "label") extends Evaluator with HasInputCol with HasInputCols {

  def this(dataset: Dataset[_], inputCol: String, metricName: String) = {
    this()
    this.inputCol = inputCol
    this.metricName = metricName
    this.dataset = dataset
  }

  def this(dataset: Dataset[_], inputCol: String) = {
    this(dataset, inputCol, "coverage")
  }

  def this(dataset: Dataset[_], inputCol: String, metricName: String, labelColName: String) = {
    this(labelColName)
    this.inputCol = inputCol
    this.metricName = metricName
    this.dataset = dataset
  }

  def this(dataset: Dataset[_], inputCols: Array[String], metricName: String) = {
    this()
    this.inputCols = inputCols
    this.metricName = metricName
    this.dataset = dataset
  }

  def this(dataset: Dataset[_], inputCols: Array[String]) = {
    this(dataset, inputCols, "coverage")
  }

  def this(dataset: Dataset[_], inputCols: Array[String], metricName: String, labelColName: String) = {
    this(labelColName)
    this.inputCols = inputCols
    this.metricName = metricName
    this.dataset = dataset
  }

  def setInputCol(inputCol: String): StatisticsEvaluator = {
    this.inputCol = inputCol
    this
  }

  def setInputCols(inputCols: Array[String]): StatisticsEvaluator = {
    this.inputCols = inputCols
    this
  }

  def setLabelColName(value: String): StatisticsEvaluator = {
    labelColName = value
    this
  }

  def getLabelColName = labelColName

  lazy val variance: Double = {
    0.0
  }

  def evaluate: Double = {
    val metric = metricName match {
      case "coverage" => getCoverage
      case "corr" => getCorrelation
      case _ => throw new IllegalArgumentException("Metric name is mismatch.")
    }
    metric
  }

  def getCoverage: Double = {
    val cnt = dataset.count()
    val notNullCnt = dataset.filter(s"$inputCol != -1").count()
    notNullCnt.toDouble / cnt
  }

  def getBasicStatistics: Array[String] = {
    dataset.filter(s"$inputCol != -1").describe(inputCol).collect().map(_.toString())
  }

  def getCorrelation: Double = {
    val datasetTmp = SchemaUtils.multiCheckNumericTypeAndCast2DoubleType(dataset, Seq(inputCol, labelColName))
    datasetTmp.stat.corr(inputCol, labelColName)
  }

  def getTopFreqBySupport(support: Double): Seq[Double] = {
    val item = dataset.stat.freqItems(Array(inputCol), support).collect().head // TODO: inputCols
    item.getSeq[Double](0)
  }

  def getTopFreqCrossLabelBySupport(support: Double): Seq[Double] = {
    val crossColName = inputCol + "_" + labelColName // TODO: inputCols
    val item = dataset.withColumn(crossColName, struct(inputCol, labelColName)).stat.freqItems(Array(crossColName), support).collect().head
    item.getSeq[Double](0)
  }

  def getQuantiles(probabilities: Seq[Double] = Seq(0.1, 0.25, 0.5, 0.75, 0.9), relativeError: Double = 0.01): Seq[Double] = {
    val datasetTmp = SchemaUtils.checkNumericTypeAndCast2DoubleType(dataset, inputCol)
    StatFunctions.multipleApproxQuantiles(datasetTmp.toDF(), Seq(inputCol), probabilities, relativeError).head // TODO: inputCols
  }

  /**
    * Get Chi-square test between label and inputCols
    *
    * @return
    */
  def getChiSquareTest: Array[(String, Double)] = {
    val toFeatureVector = udf { row: Row =>
      val fValues = inputCols.map { colName =>
        val fieldIdx = row.fieldIndex(colName)
        try {
          row.getDouble(fieldIdx)
        } catch {
          case e: Exception => -1.0 // 如果为NULL, 则返回-1
        }
      }
      Vectors.dense(fValues)
    }

    val datasetTmp = SchemaUtils.multiCheckNumericTypeAndCast2DoubleType(dataset, inputCols)
    val input: RDD[LabeledPoint] =
      datasetTmp.select(col(labelColName).cast(DoubleType), toFeatureVector(struct(inputCols.map(col): _*)).as("features")).rdd.map {
        case Row(label: Double, features: Vector) => LabeledPoint(label, features)
      }
    val chiSqTestResult = Statistics.chiSqTest(input).zipWithIndex
    chiSqTestResult.sortBy { case (res, _) => res.pValue }.map { case (res, index) => (inputCols(index), res.pValue) }
  }

}

object StatisticsEvaluator {
  def apply(dataset: Dataset[_], inputCol: String): StatisticsEvaluator = new StatisticsEvaluator(dataset, inputCol)

  def apply(dataset: Dataset[_], inputCol: String, metricName: String): StatisticsEvaluator = new StatisticsEvaluator(dataset, inputCol, metricName)

  def apply(dataset: Dataset[_], inputCol: String, metricName: String, labelColName: String): StatisticsEvaluator = new StatisticsEvaluator(dataset, inputCol, metricName, labelColName)

  def apply(dataset: Dataset[_], inputCols: Array[String]): StatisticsEvaluator = new StatisticsEvaluator(dataset, inputCols)

  def apply(dataset: Dataset[_], inputCols: Array[String], metricName: String): StatisticsEvaluator = new StatisticsEvaluator(dataset, inputCols, metricName)

  def apply(dataset: Dataset[_], inputCols: Array[String], metricName: String, labelColName: String): StatisticsEvaluator = new StatisticsEvaluator(dataset, inputCols, metricName, labelColName)
}

