package org.fepipeline.feature.subsample.rdd

import org.apache.spark.rdd.RDD

/**
  * NegativeSubsampler: filter infoids which have a few pv and a few click or some pv and some click, only retain
  * infoids which are intense positive or negative.
  *
  * @param infoidIdx
  * @param labelIdx
  * @param clickMax infoid's click maximum
  * @param pvMax  infoid's pv maximum
  * @param ctrMinMax ctr (click / pv) on each infoid, but it is hard to define.
  * @param sampleStatFlag
  * @param reducePartitionNum
  * @param separator
  */
class NegativeSubsampler(var infoidIdx: Int,
                         var labelIdx: Int,
                         var clickMax: Int = 1,
                         var pvMax: Int = 3,
                         var ctrMinMax: (Double, Double) = (0.08, 0.12),
                         var sampleStatFlag: Boolean = false,
                         var reducePartitionNum: Int = -1,
                         val separator: String = "\001") extends RDDSubsampler {

  def subsample(sampleRDD: RDD[Array[String]]): RDD[Array[String]] = {

    val infoidsFilter = sampleRDD.mapPartitions { iter =>
      iter.flatMap { line =>
        val infoid = line(infoidIdx)
        if (infoid.length > 3) {
          val label = line(labelIdx).toInt
          Some(line(infoidIdx), (label, 1 - label))
        } else None
      }
    }.reduceByKey { case (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) }
      .filter { case (infoid, (clickNum, noClickNum)) =>
        val pvNum = clickNum + noClickNum
        val ctr = clickNum.toDouble / pvNum
        !(pvNum < pvMax && clickNum < clickMax) && !(ctr > ctrMinMax._1 && ctr < ctrMinMax._2)
      }.map(elem => elem._1)
      .collect().toSet

    val sc = sampleRDD.sparkContext
    val infoidsFilterBC = sc.broadcast(infoidsFilter)
    val subsampleRDD = sampleRDD.filter { line =>
      val infoidsFilter = infoidsFilterBC.value
      infoidsFilter.contains(line(infoidIdx))
    }

    if (sampleStatFlag) {
      outputStatInfo(sampleRDD.map(_ (labelIdx).toShort), subsampleRDD.map(_ (labelIdx).toShort), this.getClass.getSimpleName)
    }

    subsampleRDD
  }
}

object NegativeSubsampler {

  def apply(infoidIdx: Int, labelIdx: Int, clickMax: Int, pvMax: Int, ctrMinMax: (Double, Double), sampleStatFlag: Boolean, reducePartitionNum: Int, separator: String): NegativeSubsampler = new NegativeSubsampler(infoidIdx, labelIdx, clickMax, pvMax, ctrMinMax, sampleStatFlag, reducePartitionNum, separator)

  def apply(infoidIdx: Int, labelIdx: Int, clickMax: Int, pvMax: Int, ctrMinMax: (Double, Double), sampleStatFlag: Boolean): NegativeSubsampler = new NegativeSubsampler(infoidIdx, labelIdx, clickMax, pvMax, ctrMinMax, sampleStatFlag)

  def apply(infoidIdx: Int, labelIdx: Int, sampleStatFlag: Boolean): NegativeSubsampler = new NegativeSubsampler(infoidIdx, labelIdx, sampleStatFlag = sampleStatFlag)

  def apply(infoidIdx: Int, labelIdx: Int, clickMax: Int, pvMax: Int, ctrMinMax: (Double, Double)): NegativeSubsampler = new NegativeSubsampler(infoidIdx, labelIdx, clickMax, pvMax, ctrMinMax)

  def apply(infoidIdx: Int, labelIdx: Int, clickMax: Int, pvMax: Int): NegativeSubsampler = new NegativeSubsampler(infoidIdx, labelIdx, clickMax, pvMax)

  def apply(infoidIdx: Int, labelIdx: Int): NegativeSubsampler = new NegativeSubsampler(infoidIdx, labelIdx)

}
