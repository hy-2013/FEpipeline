package org.fepipeline.feature

import org.fepipeline.feature.assemble.Assembler
import org.fepipeline.feature.conversion.TitleLenConvertor
import org.fepipeline.feature.discretization.SameNumDiscretizer
import org.fepipeline.feature.stat.{Aggregater, CXRStater}
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class OutputFormaterTestCase extends BaseTestSuite {
  test("OutputFormater") {
    val headElems = Array("imei", "cookieid", "userid", "platform", "ptype", "slot", "infoid", "sid", "label")
    val colNameSetids = Array("score" -> 1)

    val outputFormater = new FtrlOutputFormater()
      .setInputCols(headElems)
      .setColNameIndexes(colNameSetids)
      .setOutputCol("sample")

    val sample = outputFormater.transform(rawSample)
    sample.show(false)
  }

  test("PipelineDiscretizerApply") {
    val colNameSetids = Array("age"-> 1, "score" -> 2, "name_len"-> 3, "age_score_name" -> 4, "age_cnt" -> 5,
      "score_ctr" -> 6, "score_ctr_disc" -> 7, "score_disc" -> 8)
    val pipeline = Pipeline(Array(
      OneEqualOneSubsampler("app", "platform"),
      TitleLenConvertor("name", "name_len"),
      Assembler(Array("age", "score", "name_len"), "age_score_name"),
      Aggregater("age", "age_cnt"),
      CXRStater("label", "score", "score_ctr"),
      SameNumDiscretizer("score", "score_disc", 3),
      SameNumDiscretizer("score_ctr", "score_ctr_disc", 3),
      FtrlOutputFormater(colNameSetids, "sample")))

    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)
    sample.show(false)
  }

}
