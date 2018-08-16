package org.fepipeline.feature.subsample.rdd

import org.apache.spark.rdd.RDD

/**
  * RandomSubsampler
  *
  * @param labelIdx
  * @param positiveRatio
  * @param negativeRatio
  * @param sampleStatFlag
  */
class RandomSubsampler(var labelIdx: Int,
                       var positiveRatio: Double,
                       var negativeRatio: Double,
                       var sampleStatFlag: Boolean = false) extends RDDSubsampler {

  def this(labelIdx: Int) {
    this(labelIdx, 0.5, 0.5)
  }

  def this(labelIdx: Int, negativeRatio: Double) {
    this(labelIdx, 1 - negativeRatio, negativeRatio)
  }

  def subsample(sampleRDD: RDD[Array[String]]): RDD[Array[String]] = {

    val (positiveRatioNew, negativeRatioNew) = getNewRatio(positiveRatio, negativeRatio)

    val sampleLabelRDD = sampleRDD.map(_ (labelIdx).toShort)
    val sampleNum = sampleLabelRDD.count()
    val sampleNegative = sampleRDD.filter(_ (labelIdx) == "0")
    val sampleNegativeNum = sampleNegative.map(_ (labelIdx).toShort).count()
    val subsampleRDD = if ((sampleNegativeNum.toDouble / sampleNum) > negativeRatioNew) {
      val negativeSampleRatio = ((sampleNum - sampleNegativeNum) * negativeRatioNew) / (positiveRatioNew * sampleNegativeNum)
      val subsampleNegative = sampleNegative.sample(false, negativeSampleRatio)
      subsampleNegative.union(sampleRDD.filter(_ (labelIdx) == "1"))
    } else {
      sampleRDD
    }

    if (sampleStatFlag) {
      outputStatInfo(sampleRDD.map(_ (labelIdx).toShort), subsampleRDD.map(_ (labelIdx).toShort), this.getClass.getSimpleName)
    }

    subsampleRDD
  }

}

object RandomSubsampler {
  def apply(labelIdx: Int, positiveRatio: Double, negativeRatio: Double, sampleStatFlag: Boolean): RandomSubsampler = new RandomSubsampler(labelIdx, positiveRatio, negativeRatio, sampleStatFlag)

  def apply(labelIdx: Int, positiveRatio: Double, negativeRatio: Double): RandomSubsampler = new RandomSubsampler(labelIdx, positiveRatio, negativeRatio)

  def apply(labelIdx: Int, negativeRatio: Double): RandomSubsampler = new RandomSubsampler(labelIdx, negativeRatio)

  def apply(labelIdx: Int): RandomSubsampler = new RandomSubsampler(labelIdx)

}
