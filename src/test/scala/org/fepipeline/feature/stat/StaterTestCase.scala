package org.fepipeline.feature.stat

import org.fepipeline.feature.BaseTestSuite
import org.fepipeline.feature.assemble.Assembler
import org.fepipeline.feature.conversion.TitleLenConvertor
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler
import org.fepipeline.feature.{Pipeline, PipelineModel}

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class StaterTestCase extends BaseTestSuite {

  test("Aggregater") {
    rawSample.printSchema()
    rawSample.show(false)
    val aggregater = new Aggregater()
      .setInputCol("age")
      .setOutputCol("age_cnt")

    val sample = aggregater.transform(rawSample)
    sample.show(false)
  }

  test("CXRAggregater") {
    val cxrStater = new CXRStater()
      .setLabelColName("label")
      .setInputCol("age")
      .setOutputCol("age_ctr")

    val sample = cxrStater.transform(rawSample)
    sample.show(false)
  }

  test("PipelineStater") {
    val subsampler = new OneEqualOneSubsampler()
      .setFilterValue("app")
      .setInputCols("platform")
    val convertor = new TitleLenConvertor()
      .setInputCol("name")
      .setOutputCol("name_len")
    val assembler = new Assembler()
      .setInputCols("age", "score", "name_len")
      .setOutputCol("age_score_name")
    val aggregater = new Aggregater()
      .setInputCol("age")
      .setOutputCol("age_cnt")
    val cxrStater = new CXRStater()
      .setLabelColName("label")
      .setInputCol("score")
      .setOutputCol("score_ctr")

    val pipeline = new Pipeline()
      .setStages(Array(subsampler, convertor, assembler, aggregater, cxrStater))
    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)

    sample.show(false)
  }

  test("PipelineStaterApply") {
    val pipelineModel = PipelineModel(Array(
      OneEqualOneSubsampler("app", "platform"),
      TitleLenConvertor("name", "name_len"),
      Assembler(Array("age", "score", "name_len"), "age_score_name"),
      Aggregater("age", "age_cnt"),
      CXRStater("label", "score", "score_ctr")))

    val sample = pipelineModel.transform(rawSample)
    sample.show(false)
  }

}
