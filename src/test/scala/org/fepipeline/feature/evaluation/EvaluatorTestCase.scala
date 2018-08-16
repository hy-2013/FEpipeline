package org.fepipeline.feature.evaluation

import org.fepipeline.feature.BaseTestSuite

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class EvaluatorTestCase extends BaseTestSuite {

  test("StatisticsEvaluator") {
//    val evaluator = StatisticsEvaluator(rawSample, "score", "coverage")
    val evaluator = StatisticsEvaluator(rawSample, Array("score", "i_lon"))
//    val evaluator = StatisticsEvaluator(rawSample, "score", "corr")

//    val scoreCoverage = evaluator.evaluate
////    val scoreCorrelation = evaluator.evaluate
//    val scoreCorrelation = evaluator.getCorrelation
//    val scoreBasicStatisties = evaluator.getBasicStatistics
//    val scoreQuantiles = evaluator.getQuantiles()
//    val scoreTopFreq = evaluator.getTopFreqBySupport(0.2)
//    val scoreTopFreqCrossLabel = evaluator.getTopFreqCrossLabelBySupport(0.1)
    val scoreChiSquareTest = evaluator.getChiSquareTest

//    println("scoreCoverage: " + scoreCoverage)
//    println("scoreCorrelation: " + scoreCorrelation)
//    println("scoreBasicStatisties: " + scoreBasicStatisties.mkString("\n"))
//    println("scoreQuantiles: " + scoreQuantiles.mkString(", "))
//    println("scoreTopFreq: " + scoreTopFreq.mkString(", "))
//    println("scoreTopFreqCrossLabel: " + scoreTopFreqCrossLabel.mkString(", "))
    println("scoreChiSquareTest: " + scoreChiSquareTest.mkString("\n"))
  }

  test("InformationEvaluator") {
//    val evaluator = InformationEvaluator(rawSample, "label", "i_lon", "infoGain")
    val evaluator = InformationEvaluator(rawSample, "i_lon", "infoGainRatio")
    val scoreInfoGain = evaluator.evaluate

//    val scoreInfoValue = evaluator.setMetricName("infoValue").evaluate
//    val scoreMutualInformation = evaluator.setMetricName("mutualInformation").evaluate

//    println("sampleStatList: " + evaluator.sampleStatList.mkString("\n"))
    println("scoreInfoGain: " + scoreInfoGain)
//    println("scoreInfoGainRatio: " + scoreInfoGainRatio)

//    println("scoreInfoValue: " + scoreInfoValue)
//    println("scoreMutualInformation: " + scoreMutualInformation)
  }

}
