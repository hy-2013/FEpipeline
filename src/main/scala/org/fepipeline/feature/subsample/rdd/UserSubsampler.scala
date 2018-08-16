package org.fepipeline.feature.subsample.rdd

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * UserSubsampler: filter users which have huge pv but a few click.
  *
  * @param useridIdx
  * @param labelIdx
  * @param sidIdx
  * @param pvMin
  * @param ctrThreshold ctr (click / pv) on every userid
  * @param sampleStatFlag
  * @param aggregatePartitionNum
  * @param separator
  */
class UserSubsampler(var useridIdx: Int,
                     var imeiIdx: Int,
                     var cookieIdx: Int,
                     var labelIdx: Int,
                     var sidIdx: Int,
                     var pvMin: Int = 50,
                     var ctrThreshold: Double = 0.06,
                     var sampleStatFlag: Boolean = false,
                     var aggregatePartitionNum: Int = -1,
                     val separator: String = "\001") extends RDDSubsampler {

  def subsample(sampleRDD: RDD[Array[String]]): RDD[Array[String]] = {

    val userRDD = sampleRDD.filter(_ (useridIdx).length >= 3)
    val noUserRDD = sampleRDD.filter(_ (useridIdx).length < 3)
    val imeiRDD = noUserRDD.filter(_ (imeiIdx).length >= 3)
    val cookieRDD = noUserRDD.filter(line => line(imeiIdx).length < 3 && line(cookieIdx).length > 3)

    val usersFilter = filter(userRDD)
    val imeisFilter = filter(imeiRDD)
    val cookiesFilter = filter(cookieRDD)

    val sc = sampleRDD.sparkContext
    val usersFilterBC = sc.broadcast(usersFilter)
    val imeisFilterBC = sc.broadcast(imeisFilter)
    val cookiesFilterBC = sc.broadcast(cookiesFilter)
    val subsampleRDD = sampleRDD.filter { line =>
      val usersFilter = usersFilterBC.value
      val imeisFilter = imeisFilterBC.value
      val cookiesFilter = cookiesFilterBC.value
      usersFilter.contains(line(useridIdx)) || imeisFilter.contains(line(imeiIdx)) || cookiesFilter.contains(line(cookieIdx))
    }

    if (sampleStatFlag) {
      outputStatInfo(sampleRDD.map(_ (labelIdx).toShort), subsampleRDD.map(_ (labelIdx).toShort), this.getClass.getSimpleName)
    }

    subsampleRDD
  }

  def filter(uidRDD: RDD[Array[String]]): Set[String] = {
    uidRDD.mapPartitions { iter =>
      iter.map { line =>
        (line(useridIdx), (line(labelIdx).toInt, line(sidIdx)))
      }
    }.aggregateByKey((0, mutable.Set[String]()))(
      seqOp = (merge: (Int, mutable.Set[String]), value: (Int, String)) => {
        (merge._1 + value._1, merge._2 + value._2)
      },
      combOp = (comb1: (Int, mutable.Set[String]), comb2: (Int, mutable.Set[String])) => (comb1._1 + comb2._1, comb1._2 ++ comb2._2)
    ).filter { case (userid, (clickNum, sids)) =>
      val pvNum = sids.size
      !(pvNum >= pvMin && (clickNum.toDouble / pvNum <= ctrThreshold))
    }.map(elem => elem._1)
      .collect().toSet
  }
}

object UserSubsampler {

  def apply(useridIdx: Int, labelIdx: Int, imeiIdx: Int, cookieIdx: Int, sidIdx: Int, pvMin: Int, ctrThreshold: Double, sampleStatFlag: Boolean, aggregatePartitionNum: Int, separator: String): UserSubsampler = new UserSubsampler(useridIdx, imeiIdx, cookieIdx, labelIdx, sidIdx, pvMin, ctrThreshold, sampleStatFlag, aggregatePartitionNum, separator)

  def apply(useridIdx: Int, imeiIdx: Int, cookieIdx: Int, labelIdx: Int, sidIdx: Int, pvMin: Int, ctrThreshold: Double, sampleStatFlag: Boolean): UserSubsampler = new UserSubsampler(useridIdx, imeiIdx, cookieIdx, labelIdx, sidIdx, pvMin, ctrThreshold, sampleStatFlag)

  def apply(useridIdx: Int, imeiIdx: Int, cookieIdx: Int, labelIdx: Int, sidIdx: Int, sampleStatFlag: Boolean): UserSubsampler = new UserSubsampler(useridIdx, imeiIdx, cookieIdx, labelIdx, sidIdx, sampleStatFlag = sampleStatFlag)

  def apply(useridIdx: Int, imeiIdx: Int, cookieIdx: Int, labelIdx: Int, sidIdx: Int, pvMin: Int, ctrThreshold: Double): UserSubsampler = new UserSubsampler(useridIdx, imeiIdx, cookieIdx, labelIdx, sidIdx, pvMin, ctrThreshold)

  def apply(useridIdx: Int, imeiIdx: Int, cookieIdx: Int, labelIdx: Int, sidIdx: Int): UserSubsampler = new UserSubsampler(useridIdx, imeiIdx, cookieIdx, labelIdx, sidIdx)

}
