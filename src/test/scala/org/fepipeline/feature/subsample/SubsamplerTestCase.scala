package org.fepipeline.feature.subsample

import org.fepipeline.feature.BaseTestSuite
import org.fepipeline.feature.Pipeline
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler
import org.fepipeline.feature.subsample.rdd.{NegativeSubsampler, RandomSubsampler, SkipAboveSubsampler, UserSubsampler}

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class SubsamplerTestCase extends BaseTestSuite {

  test("OneEqualOneSubsampler"){
    rawSample.printSchema()
    val subsampler = new OneEqualOneSubsampler()
      .setFilterValue("app")
      .setInputCols("platform")

    val sample = subsampler.transform(rawSample)
    sample.show(false)
  }

  test("RandomSubsampler"){
//    val subsampler = RandomSubsampler(3, 0.5)
//    val subsampler = RandomSubsampler(3, 1.0, 2.0)
    val subsampler = RandomSubsampler(3, 1.0, 2.0, true)

    val sample = subsampler.transform(rawSample)
    sample.show(false)
  }

  test("SkipAboveSubsampler"){
    val subsampler = SkipAboveSubsampler(2, 3, Array(11), 16, true)

    val sample = subsampler.transform(rawSample)
    sample.show(false)
  }

  test("UserSubsampler"){
    val subsampler: Subsampler = UserSubsampler(7, 5, 6, 3, 11, 3, 0.1, true)

    val sample = subsampler.transform(rawSample)
    sample.show(false)
  }

  test("NegativeSubsampler"){
    val subsampler: Subsampler = NegativeSubsampler(10, 3, 1, 3)

    val sample = subsampler.transform(rawSample)
    sample.show(false)
  }

  test("MultiSubsampler"){
    val pipeline = Pipeline(Array(
      OneEqualOneSubsampler("28", "age"),
      SkipAboveSubsampler(2, 3, Array(11), 16, true),
      UserSubsampler(7, 5, 6, 3, 11, 3, 0.1, true),
      NegativeSubsampler(10, 3, 1, 3)
    ))

    val pipelineModel = pipeline.fit(rawSample)
    val sampleFilter = pipelineModel.transform(rawSample)
    sampleFilter.show(false)
  }


}
