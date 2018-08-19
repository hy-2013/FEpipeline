package org.fepipeline.job

import org.fepipeline.feature.assemble.Assembler
import org.fepipeline.feature.conversion.TitleLenConvertor
import org.fepipeline.feature.discretization.SameNumDiscretizer
import org.fepipeline.feature.evaluation.{InformationEvaluator, StatisticsEvaluator}
import org.fepipeline.feature.stat.{Aggregater, CXRStater}
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler
import org.fepipeline.feature.{FtrlOutputFormater, Pipeline}
import org.apache.spark.sql.SparkSession

/**
  * FEpipelineExample.
  *
  * @author Zhang Chaoli
  */
object FEpipelineExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("[Huye] FEpipelineExample")
      .enableHiveSupport()
      .getOrCreate()

    val outPath = args(0)
    val rawSample = spark.table("raw_sample")

    // Define the map between column name and feature index for FtrlOutputFormater.
    val colNameIndexes = Array("age"-> 1, "score" -> 2, "title_len"-> 3, "age_score_titlelen" -> 4,
      "pv" -> 5, "infoid_ctr" -> 6, "title_len_disc" -> 7, "infoid_ctr_disc" -> 8)

    // Define the feature pipeline.
    val pipeline = Pipeline(Array(
      OneEqualOneSubsampler("app", "platform"),
      TitleLenConvertor("title", "title_len"),
      Assembler(Array("age", "score", "title_len"), "age_score_titlelen"),
      Aggregater("infoid", "pv"),
      CXRStater("label", "infoid", "infoid_ctr"),
      SameNumDiscretizer("title_len", "title_len_disc", 5),
      SameNumDiscretizer("infoid_ctr", "infoid_ctr_disc", 10),
      FtrlOutputFormater(colNameIndexes, "sample")))

    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)

    sample.write.save(outPath)

    // Feature evaluation
    val statisticsEvaluator = StatisticsEvaluator(rawSample, "age", "coverage")
    val coverage = statisticsEvaluator.evaluate
    val informationEvaluator = InformationEvaluator(rawSample, "label", "age", "infoGain")
    val infoGain = informationEvaluator.evaluate

  }
}
