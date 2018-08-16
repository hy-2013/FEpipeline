package org.fepipeline.feature.subsample.rdd

import org.fepipeline.feature.subsample.Subsampler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BooleanType, ByteType, _}
import org.apache.spark.sql.{Row, _}

import scala.collection.mutable

/**
  * RDDSubsampler.
  *
  * @author Zhang Chaoli
  */
abstract class RDDSubsampler extends Subsampler {

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val subsampleRDD = subsample(dataset.rdd.map(_.asInstanceOf[Row].toSeq.map(_.toString).toArray))
    val schemaTmp = dataset.schema
    val subsampleRDDTrans = subsampleRDD.map { arr =>
      val arrTrans = mutable.ArrayBuffer[Any]()
      var i = 0
      schemaTmp.foreach { sf =>
        sf.dataType match {
          case d: DoubleType => arrTrans += arr(i).toDouble
          case f: FloatType => arrTrans += arr(i).toFloat
          case it: IntegerType => arrTrans += arr(i).toInt
          case l: LongType => arrTrans += arr(i).toLong
          case s: ShortType => arrTrans += arr(i).toShort
          case byte: ByteType => arrTrans += arr(i).toByte
          case bool: BooleanType => arrTrans += arr(i).toBoolean
          case _ => arrTrans += arr(i)
        }
        i += 1
      }
      arrTrans.toArray
    }
    dataset.sparkSession.createDataFrame(subsampleRDDTrans.map(Row(_: _*)), dataset.schema)
  }

  /**
    * Customize subsample strategy.
    *
    * @param sampleRDD
    * @return subsampled rdd, which should be the same field with input sampleRDD.
    */
  protected def subsample(sampleRDD: RDD[Array[String]]): RDD[Array[String]]

}
