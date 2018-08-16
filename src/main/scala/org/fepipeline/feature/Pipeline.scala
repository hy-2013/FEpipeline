package org.fepipeline.feature

import org.fepipeline.common.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

/**
  * PipelineStage.
  *
  * @author Zhang Chaoli
  */
trait PipelineStage extends Logging with Serializable {

  def transformSchema(schema: StructType): StructType

  protected def transformSchema(schema: StructType, logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }
}

/**
  * Pipeline
  *
  * @param stages
  */
class Pipeline(var stages: Array[PipelineStage]) extends Trainer[PipelineModel] {

  def this() = this(null)

  def setStages(value: Array[_ <: PipelineStage]): this.type = {
    stages = value.asInstanceOf[Array[PipelineStage]]
    this
  }

  def getStages: Array[PipelineStage] = stages

  def fit(dataset: Dataset[_]): PipelineModel = {
//    transformSchema(dataset.schema, logging = true)
    // Search for the last estimator (so that reduce the following if judge).
    var indexOfLastTrainer = -1
    stages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Trainer[_] =>
          indexOfLastTrainer = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    stages.view.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastTrainer) {
        val transformer = stage match {
          case trainer: Trainer[_] =>
            trainer.fit(curDataset)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Does not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastTrainer) {
          curDataset = transformer.transform(curDataset)
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }

    PipelineModel(transformers.toArray)
  }

  override def transformSchema(schema: StructType): StructType = {
    val theStages = stages
    require(theStages.toSet.size == theStages.length,
      "Cannot have duplicate components in a pipeline.")
    theStages.foldLeft(schema)((cur, stage) => stage.transformSchema(cur))
  }
}

object Pipeline {
  def apply(stages: Array[PipelineStage]): Pipeline = new Pipeline(stages)
}

class PipelineModel(val stages: Array[Transformer]) extends Model[PipelineModel] {
  override def transform(dataset: Dataset[_]): DataFrame = {
//    transformSchema(dataset.schema, logging = true)
    stages.foldLeft(dataset.toDF)((cur, transformer) => transformer.transform(cur))
  }

  override def transformSchema(schema: StructType): StructType = {
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
  }
}

object PipelineModel{
  def apply(stages: Array[Transformer]): PipelineModel = new PipelineModel(stages)
}