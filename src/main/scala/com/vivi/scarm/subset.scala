package com.vivi.scarm

import shapeless._

trait Subset[A,SET<:HList]

trait LowerPrioritySubset {
  //conflicst with hnilIsSubset
  implicit def containedInTail[A,HD,TAIL<:HList]
    (implicit subset: Subset[A,TAIL])
      :Subset[A,HD::TAIL] = new Subset[A,HD::TAIL] {}

}

object Subset extends LowerPrioritySubset {
  def apply[A,SET<:HList]
    (implicit subset: Subset[A,SET]): Subset[A,SET] = subset

  implicit def hnilIsSubset[SET<:HList]: Subset[HNil,SET] = new Subset[HNil,SET] {}

  implicit def headAndTail[A,TAIL<:HList,SET<:HList]
  (implicit hd: Subset[A,SET], tl: Subset[TAIL,SET]): Subset[A::TAIL,SET] =
    new Subset[A::TAIL,SET] {}

  implicit def containedInHead[A,TAIL<:HList]: Subset[A,A::TAIL] =
    new Subset[A,A::TAIL] {}
}
