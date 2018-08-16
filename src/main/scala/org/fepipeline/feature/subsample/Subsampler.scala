package org.fepipeline.feature.subsample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.fepipeline.common.Logging
import org.fepipeline.feature._

/**
  * Subsampler.
  *
  * @author Zhang Chaoli
  */
abstract class Subsampler extends Transformer with Serializable with Logging {

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame

  /**
    *
    * @param sample    RDD[label]
    * @param subsample RDD[label]
    * @param sampleType
    */
  def outputStatInfo(sample: RDD[Short], subsample: RDD[Short], sampleType: String): Unit = {
    val allNum = sample.count()
    val positiveNum = sample.filter(_ == 1).count()
    val negativeNum = allNum - positiveNum
    val subNum = subsample.count()
    val positiveSubNum = subsample.filter(_ == 1).count()
    val negativeSubNum = subNum - positiveSubNum
    println(s"\n###########################################################################\n" +
      s"$sampleType\t样本总数, 负样本总数, 正样本总数, 负正样本比例\n" +
      s"All Pos_Neg: $allNum, $negativeNum, $positiveNum, ${negativeNum.toDouble / positiveNum}\n\n" +
      s"Sub Pos_Neg: $subNum, $negativeSubNum, $positiveSubNum, ${negativeSubNum.toDouble / positiveSubNum}\n\n" +
      s"###########################################################################\n")
  }

  def getNewRatio(positiveRatio: Double, negativeRatio: Double) = {
    if (positiveRatio + negativeRatio != 1.0) {
      val sum = positiveRatio + negativeRatio
      (positiveRatio / sum, negativeRatio / sum)
    } else {
      (positiveRatio, negativeRatio)
    }
  }
}
