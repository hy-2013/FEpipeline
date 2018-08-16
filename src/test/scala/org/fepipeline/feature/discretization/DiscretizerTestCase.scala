package org.fepipeline.feature.discretization

import org.fepipeline.feature.BaseTestSuite
import org.fepipeline.feature.assemble.Assembler
import org.fepipeline.feature.conversion.TitleLenConvertor
import org.fepipeline.feature.stat.{Aggregater, CXRStater}
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler
import org.fepipeline.feature.Pipeline

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class DiscretizerTestCase extends BaseTestSuite {

  test("SameNumDiscretizer") {
    rawSample.printSchema()
    rawSample.show(false)
    val discretizer = new SameNumDiscretizer()
      .setInputCol("score")
      .setOutputCol("score_disc")
      .setBoundNum(3)

    val boundsDiscretizerModel = discretizer.fit(rawSample)

    val sample = boundsDiscretizerModel.transform(rawSample)
    println(boundsDiscretizerModel.getBounds.mkString("\t"))
    sample.show(false)
  }

  test("PipelineDiscretizer") {
    rawSample.printSchema()
    rawSample.show(false)
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
    val discretizer = new SameNumDiscretizer()
      .setInputCol("score_ctr")
      .setOutputCol("score_ctr_disc")
      .setBoundNum(3)

    val pipeline = new Pipeline()
      .setStages(Array(subsampler, convertor, assembler, aggregater, cxrStater, discretizer))
    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)

    sample.show(false)
  }

  test("PipelineDiscretizerApply") {
    val pipeline = Pipeline(Array(
      OneEqualOneSubsampler("app", "platform"),
      TitleLenConvertor("name", "name_len"),
      Assembler(Array("age", "score", "name_len"), "age_score_name"),
      Aggregater("age", "age_cnt"),
      CXRStater("label", "score", "score_ctr"),
      SameNumDiscretizer("score_ctr", "score_ctr_disc", 3)))

    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)
    sample.show(false)
  }

}
