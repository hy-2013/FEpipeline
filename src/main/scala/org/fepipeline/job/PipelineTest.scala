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
  * PipelineTest.
  *
  * @author Zhang Chaoli
  */
object PipelineTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("[Huye] PipelineTest")
      .enableHiveSupport()
      .getOrCreate()

    val outPath = args(0)
    val rawSample = spark.table("raw_sample")

    val colNameIndexes = Array("age"-> 1, "score" -> 2, "name_len"-> 3, "age_score_titlelen" -> 4,
      "pv" -> 5, "item_ctr" -> 6, "name_len_disc" -> 7, "item_ctr_disc" -> 8)

    val pipeline = Pipeline(Array(
      OneEqualOneSubsampler("app", "platform"),
      TitleLenConvertor("title", "name_len"),
      Assembler(Array("age", "score", "name_len"), "age_score_titlelen"),
      Aggregater("item", "pv"),
      CXRStater("label", "item", "item_ctr"),
      SameNumDiscretizer("name_len", "name_len_disc", 5),
      SameNumDiscretizer("item_ctr", "item_ctr_disc", 10),
      FtrlOutputFormater(colNameIndexes, "sample")))

    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)

    sample.write.save(outPath)

    val statisticsEvaluator = StatisticsEvaluator(rawSample, "age", "coverage")
    val coverage = statisticsEvaluator.evaluate
    val informationEvaluator = InformationEvaluator(rawSample, "label", "age", "infoGain")
    val infoGain = informationEvaluator.evaluate

  }
}
