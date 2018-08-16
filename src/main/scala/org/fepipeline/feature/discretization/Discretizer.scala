package org.fepipeline.feature.discretization

import org.fepipeline.feature._
import org.fepipeline.util.SchemaUtils
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

/**
  * DiscretizerBase.
  *
  * @author Zhang Chaoli
  */

private[discretization] trait DiscretizerBase extends HasInputCol with HasOutputCol {

  protected def validateAndTransformSchema(schema: StructType, inputColDataType: DataType, outputColDataType: DataType = DataTypes.IntegerType): StructType = {
    SchemaUtils.checkColumnType(schema, inputCol, inputColDataType)
    SchemaUtils.appendColumn(schema, outputCol, outputColDataType)
  }
}

abstract class Discretizer[M <: DiscretizerModel[M]] extends Trainer[M] with DiscretizerBase {

}

abstract class DiscretizerModel[M <: DiscretizerModel[M]] extends Model[M] with DiscretizerBase  {

}





