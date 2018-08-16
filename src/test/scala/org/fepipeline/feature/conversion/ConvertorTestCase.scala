package org.fepipeline.feature.conversion

import org.fepipeline.feature.BaseTestSuite
import org.fepipeline.feature.subsample.dataframe.OneEqualOneSubsampler
import org.fepipeline.feature.Pipeline

/**
  * ClassDescription.
  *
  * @author Zhang Chaoli
  */
class ConvertorTestCase extends BaseTestSuite {
  test("TitleLenConvertor") {
    val convertor = new TitleLenConvertor()
      .setInputCol("name")
      .setOutputCol("name_len")

    val sample = convertor.transform(rawSample)
    sample.show(false)
  }

  test("PipelineConvertor") {
    val subsampler = new OneEqualOneSubsampler()
      .setFilterValue("app")
      .setInputCols("platform")
    val convertor = new TitleLenConvertor()
      .setInputCol("name")
      .setOutputCol("name_len")

    val pipeline = new Pipeline()
      .setStages(Array(subsampler, convertor))
    val pipelineModel = pipeline.fit(rawSample)
    val sample = pipelineModel.transform(rawSample)

    sample.show(false)
    //    +--------+---+---------+-----+--------+--------+
    //    |name    |age|job      |label|platform|name_len|
    //    +--------+---+---------+-----+--------+--------+
    //    |Jorge   |30 |Developer|0    |app     |5       |
    //    |Bob     |32 |coder    |1    |app     |3       |
    //    |zhangsan|29 |Developer|0    |app     |8       |
    //    |lisi    |32 |coder    |0    |app     |4       |
    //    +--------+---+---------+-----+--------+--------+
  }

  test("LonLatDistanceConvertor") {
    val convertor = new LonLatDistanceConvertor()
      .setInputCols("u_lat", "u_lon", "i_lat", "i_lon")
      .setOutputCol("distance")

    val sample = convertor.transform(rawSample)
    sample.show(false)
  }

  test("calculateDistanceInMeter") {
    val userLocation = Location(10, 20)
    val infoLocation = Location(40, 20)

    println("----calculateDistanceInMeter: " + new LonLatDistanceConvertor().calculateDistanceInMeter(userLocation, infoLocation))
  }

}
