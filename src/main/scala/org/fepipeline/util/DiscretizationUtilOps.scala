package org.fepipeline.util

import org.fepipeline.conf.FeatureJsonConfig
import org.fepipeline._
import org.fepipeline.common.Logging

import scala.collection.mutable

/**
  * DiscretizationUtilOps.
  *
  * @author Zhang Chaoli
  */
class DiscretizationUtilOps(val discretizationUtil: DiscretizationUtil) extends Logging{
  //信息增益获取ctr分段
  def getInfoGainDiscretizeBounds(numCtrArr: Array[(Long, Long, Double)], maxDepth: Int): Array[Double] = {
    val lowIndex = 0
    val upIndex = numCtrArr.length - 1
    val infoGainTree = InfoGain.creatTree(numCtrArr, lowIndex, upIndex, maxDepth, 0)
    val boundArr = mutable.ArrayBuffer[Double]()
    val boundIndex = mutable.ArrayBuffer[Int]()
    InfoGain.midSearch(infoGainTree, boundIndex)
    boundArr += 0.0
    for (i <- boundIndex.indices) {
      boundArr += numCtrArr(boundIndex(i))._3
    }
    boundArr += 1.0
    boundArr.toArray
  }

  def getSimiCtrDiscretizeBounds(ctrArr: Array[Double], ctrSimiDist: Double): Array[Double] = {
    val boundArr = mutable.ArrayBuffer[Double]()
    val (maxCtr, minCtr) = (ctrArr.max, ctrArr.min)
    boundArr += 0.0
    val fieldNum = ((maxCtr - minCtr) / ctrSimiDist).toInt
    (0 to fieldNum).foreach(i => boundArr += minCtr + i * ctrSimiDist)
    boundArr += 1.0
    boundArr.toArray
  }

  /**
    * NOTE: if numValueArr contain -1, interval number will be n+1, and bounds will be n+2, where boundNum=n.
    * @param numValueArrTmp
    * @param boundNum
    * @return
    */
  def getSamenumDiscretizeBounds(numValueArrTmp: Array[(Long, Double)], boundNum: Int): Array[Double] = {
    val boundNumTmp = boundNum - 1
    val boundArr = mutable.LinkedHashSet[Double]()
    val numValueArr = if (numValueArrTmp(0)._2 == -1.0) { // -1单独先分为一段
      boundArr += -1.0
      numValueArrTmp.drop(1)
    } else {
      numValueArrTmp
    }
    boundArr += 0.0  // TODO: 对占比特别高的值单独分段
    val allNum = numValueArr.map(_._1).sum
    val interval = allNum.toDouble / boundNumTmp
    val boundNums = (1 to boundNumTmp).map(i => i * interval).toArray /*:+ allNum*/
    var currNum = 0L
    var currIdx = 0
    for ((num, ctr) <- numValueArr) {
      currNum += num
      (currIdx until boundNumTmp).foreach { i =>
        if (currNum >= boundNums(i)) {
          currIdx += 1
          boundArr += ctr
        }
      }
    }
    boundArr.toArray
  }

  def getFieldDiscretizeBounds(numValueArr: Array[(Long, Double)], boundNum: Int): Array[Double] = {
    // 过滤掉两端极端的数据（前后 1/boundnum 的数据）
    val allNum = numValueArr.map(_._1).sum
    val lowBoundIdx = allNum / boundNum
    val upBoundIdx = allNum - lowBoundIdx
    var lowBound = 0.0
    var lowBoundFlag = true
    var upBound = 1.0
    var upBoundFlag = true
    var currNum = 0L
    for ((num, value) <- numValueArr) {
      currNum += num
      if (currNum >= lowBoundIdx && lowBoundFlag) {
        lowBound = value
        lowBoundFlag = false
      }
      if (currNum >= upBoundIdx && upBoundFlag) {
        upBound = value
        upBoundFlag = false
      }
    }
    //    discretizationUtil.fieldDiscretize(0.8, 0.01, 10, 0.1) // FIXME:对象不能调用static方法
    getFieldDiscretizeBounds(lowBound, boundNum, upBound)
  }

  def getFieldDiscretizeBounds(lowBound: Double, boundNum: Int, upBound: Double): Array[Double] = {
    val boundArr = mutable.ArrayBuffer[Double]()
    boundArr += 0.0
    val interval = (upBound - lowBound) / (boundNum - 2)
    (0 to boundNum - 2).foreach(i => boundArr += lowBound + i * interval)
    boundArr += 1.0
    boundArr.toArray
  }

  def getFieldDiscretizeBounds(bounds: Array[Double]): Array[Double] = {
    getFieldDiscretizeBounds(bounds(0), bounds(1).toInt, bounds(2))
  }

  def getBounds(id: String, numValueArr: Array[(Long, Double)], discretizeType: String = FeatureJsonConfig.DISC_SAME_NUM, boundNum: Int = 10): Array[Double] = {
    val bounds = discretizeType match {
      case FeatureJsonConfig.DISC_EQUAL_FIELD =>
        discretizationUtil.getFieldDiscretizeBounds(numValueArr, boundNum)
      case FeatureJsonConfig.DISC_SAME_NUM =>
        discretizationUtil.getSamenumDiscretizeBounds(numValueArr, boundNum)
    }
    require(bounds.nonEmpty, "org.fepipeline.feature.samenum.discretize.bounds should NOT be null.")

    logWarning(s"\n#################################################################################\n" +
      s"$id $discretizeType bounds: ${
        bounds.mkString(", ")
      }\n" +
      s"#################################################################################\n")

    bounds
  }

}
