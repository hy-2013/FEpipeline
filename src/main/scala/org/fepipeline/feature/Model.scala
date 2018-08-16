package org.fepipeline.feature

/**
  * Model.
  *
  * @author Zhang Chaoli
  */
abstract class Model[M <: Model[M]] extends Transformer {
  @transient var parent: Trainer[M] = _

  def setParent(parent: Trainer[M]): M = {
    this.parent = parent
    this.asInstanceOf[M]
  }

  /** Indicates whether this [[Model]] has a corresponding parent. */
  def hasParent: Boolean = parent != null

}
