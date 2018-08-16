package org.fepipeline.util

import scala.collection.mutable
import org.fepipeline._


//定义二叉树
class BaseTree(index: Int, left: BaseTree, right: BaseTree) {
  val value = index
  var leftNode = left
  var rightNode = right
}

object InfoGain {

  //计算信息熵
  def cacEntropy(posNum: Long, negNum: Long): Double = {
    val sampleNum = posNum + negNum
    val p1 = posNum.toDouble / sampleNum
    val p2 = negNum.toDouble / sampleNum
    val shannoEnt = - p1 * math.log(p1) / math.log(2) - p2 * math.log(p2) / math.log(2)
    shannoEnt
  }

  //计算最大信息增益的index
  def getMaxInfoGainInex(numCtrArr: Array[(Long, Long, Double)], lowIndex: Int, upIndex: Int): Int = {
    var posSum = 0L
    var negSum = 0L
    for (i <- lowIndex to upIndex) {
      val (posNum, negNum, ctr) = numCtrArr(i)
      posSum += posNum
      negSum += negNum
    }
    val sampleSum = posSum + negSum
    //val entropySource = cacEntropy(posSum, negSum)//原始信息熵
    var entropy = 1.0
    var maxIndex = lowIndex
    for (i <- lowIndex to upIndex) {
      val tmpctr = numCtrArr(i)._3
      var frontPosSum = 0L
      var frontNegSum = 0L
      var backPosSum = 0L
      var backNegSum = 0L
      for (j <- lowIndex to upIndex) {
        //ctr分两段的正负样本统计
        val (posNum, negNum, ctr) = numCtrArr(j)
        if (ctr <= tmpctr) {
          frontPosSum += posNum
          frontNegSum += negNum
        }
      }
      backPosSum = posSum - frontPosSum
      backNegSum = negSum - frontNegSum
      val frontSum = frontPosSum + frontNegSum
      val backSum = backPosSum + backNegSum
      //该ctr分段的信息熵
      val tmpEntropy = (frontSum.toDouble / sampleSum) * cacEntropy(frontPosSum, frontNegSum) + (backSum.toDouble / sampleSum) * cacEntropy(backPosSum, backNegSum)
      if (tmpEntropy < entropy) {
        entropy = tmpEntropy
        maxIndex = i
      }
    }
    maxIndex
  }

  //构造信息增益树
  def creatTree (numCtrArr: Array[(Long, Long, Double)], lowIndex: Int, upIndex: Int, maxDepth: Int, depth: Int): BaseTree = {
    if (lowIndex >= upIndex) { //区间长度为1或者左右越界
      return null
    }
    val ndepth = depth + 1
    val bestIndex = getMaxInfoGainInex(numCtrArr, lowIndex, upIndex)
    val singleTree = new BaseTree(bestIndex, null, null)
    if (ndepth == maxDepth) {//树达到指定深度
      return singleTree
    }
    singleTree.leftNode = creatTree(numCtrArr, lowIndex, bestIndex - 1, maxDepth, ndepth);
    singleTree.rightNode = creatTree(numCtrArr, bestIndex + 1, upIndex, maxDepth, ndepth)
    return singleTree
  }

  //二叉树中序遍历
  def midSearch(tree: BaseTree, boundIndex: mutable.ArrayBuffer[Int]): Unit = {
    if (tree == null) {
      return
    }
    if (tree.leftNode != null) {
      midSearch(tree.leftNode, boundIndex)
    }
    boundIndex += tree.value
    if (tree.rightNode != null) {
      midSearch(tree.rightNode, boundIndex)
    }
  }

  def main(agrs: Array[String]): Unit = {
    val dataSet = Array((10L, 20L, 0.1), (30L, 40L, 0.2), (50L, 60L, 0.3), (70L, 80L, 0.4), (90L, 100L, 0.5), (80L, 70L, 0.6), (60L, 50L, 0.65))
    val re = (new DiscretizationUtil).getInfoGainDiscretizeBounds(dataSet,3)
    println(re.mkString(","))
  }
}