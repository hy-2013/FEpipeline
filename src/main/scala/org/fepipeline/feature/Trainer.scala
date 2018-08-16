package org.fepipeline.feature

import org.apache.spark.sql.Dataset

/**
  * Trainer.
  *
  * @author Zhang Chaoli
  */
abstract class Trainer[M <: Model[M]] extends PipelineStage {
  def fit(dataset: Dataset[_]): M

}
