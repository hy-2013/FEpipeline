package org.fepipeline.feature.subsample.rdd

import org.apache.spark.rdd.RDD

/**
  * SkipAboveSubsampler
  *
  * @param skipNum
  * @param labelIdx
  * @param uniquePVIndice sid (+ slot)
  * @param posIdx pv position
  * @param sampleStatFlag
  * @param aggregatePartitionNum
  * @param separator
  */
class SkipAboveSubsampler(var skipNum: Int,
                          var labelIdx: Int,
                          var uniquePVIndice: Array[Int],
                          var posIdx: Int,
                          var sampleStatFlag: Boolean = false,
                          var aggregatePartitionNum: Int = -1,
                          val separator: String = "\001") extends RDDSubsampler {
  
  def this(labelIdx: Int, uniquePVIndice: Array[Int], posIdx: Int) {
    this(4, labelIdx, uniquePVIndice, posIdx)
  }

  def subsample(sampleRDD: RDD[Array[String]]): RDD[Array[String]] = {
    val subsampleRDD1 = sampleRDD.mapPartitions { iter =>
      iter.map { sampleArr =>
        val label = sampleArr(labelIdx).toShort
        val pos = sampleArr(posIdx).toShort
        val uniquePvKey = new StringBuilder
        uniquePVIndice.map(idx => uniquePvKey.append(sampleArr(idx)))
        (uniquePvKey.toString, (label, pos, sampleArr.mkString(separator)))
      }
    }

    val subsampleRDD2 = if (aggregatePartitionNum <= 0) {
      subsampleRDD1.aggregateByKey(Array[(Short, Short, String)]())(_ :+ _, _ ++ _)
    } else {
      subsampleRDD1.aggregateByKey(Array[(Short, Short, String)](), aggregatePartitionNum)(_ :+ _, _ ++ _)
    }

    val subsampleRDD = subsampleRDD2.flatMap { case (uniquePvKey, elem) =>
      val sortLineArr = elem.sortBy(_._2)
      val lastPostiveIdxOption = sortLineArr.zipWithIndex.filter(_._1._1 == 1).lastOption
      val sampleLineIter = lastPostiveIdxOption match {
        case None => sortLineArr.take(skipNum - 1) // if no click, take skiptNum - 1 (not filter all).
        case Some(tuple) =>
          val lastPostiveIdx = tuple._2
          if (lastPostiveIdx + skipNum + 1 >= sortLineArr.length) {
            sortLineArr
          } else {
            sortLineArr.take(lastPostiveIdx + skipNum + 1)
          }
      }
      sampleLineIter.map { case (label, pos, line) => (label, line) }
    }

    if (sampleStatFlag) {
      outputStatInfo(sampleRDD.map(_ (labelIdx).toShort), subsampleRDD.map(_._1), this.getClass.getSimpleName)
    }

    subsampleRDD.map(_._2.split(separator, -1))
  }

}

object SkipAboveSubsampler {
  def apply(skipNum: Int, labelIdx: Int, uniquePVIndice: Array[Int], posIdx: Int, sampleStatFlag: Boolean, aggregatePartitionNum: Int, separator: String): SkipAboveSubsampler = new SkipAboveSubsampler(skipNum, labelIdx, uniquePVIndice, posIdx, sampleStatFlag, aggregatePartitionNum, separator)

  def apply(skipNum: Int, labelIdx: Int, uniquePVIndice: Array[Int], posIdx: Int, sampleStatFlag: Boolean): SkipAboveSubsampler = new SkipAboveSubsampler(skipNum, labelIdx, uniquePVIndice, posIdx, sampleStatFlag = sampleStatFlag)

  def apply(skipNum: Int, labelIdx: Int, uniquePVIndice: Array[Int], posIdx: Int): SkipAboveSubsampler = new SkipAboveSubsampler(skipNum, labelIdx, uniquePVIndice, posIdx)

  def apply(labelIdx: Int, uniquePVIndice: Array[Int], posIdx: Int): SkipAboveSubsampler = new SkipAboveSubsampler(labelIdx, uniquePVIndice, posIdx)

}
