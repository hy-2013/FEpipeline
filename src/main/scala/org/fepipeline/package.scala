package org

import org.fepipeline.util.{DiscretizationUtil, DiscretizationUtilOps}

/**
  * Created by Zhang Chaoli on 31/08/2017.
  */
package object fepipeline {
  implicit def discretization2discretizationops(discretizationUtil : DiscretizationUtil) : DiscretizationUtilOps = new DiscretizationUtilOps(discretizationUtil)

}
