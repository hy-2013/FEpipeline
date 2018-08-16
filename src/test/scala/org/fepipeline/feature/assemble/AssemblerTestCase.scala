package org.fepipeline.feature.assemble

import org.fepipeline.feature.BaseTestSuite
import org.fepipeline.feature.conversion.TitleLenConvertor
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler
import org.fepipeline.feature.Pipeline

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class AssemblerTestCase extends BaseTestSuite {

  test("Assembler") {
    rawSample.printSchema()
    val assembler = new Assembler()
      .setInputCols("age", "score")
      .setOutputCol("age_score")

    val sample = assembler.transform(rawSample)
    sample.show(false)
  }

  test("PipelineConvertor") {
    val subsampler = new OneEqualOneSubsampler()
      .setFilterValue("app")
      .setInputCols("platform")
    val convertor = new TitleLenConvertor()
      .setInputCol("name")
      .setOutputCol("name_len")
    val assembler = new Assembler()
      .setInputCols("age", "score", "name_len")
      .setOutputCol("age_score_name")

    val pipeline = new Pipeline()
      .setStages(Array(subsampler, convertor, assembler))
    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)

    sample.show(false)
  }

}
