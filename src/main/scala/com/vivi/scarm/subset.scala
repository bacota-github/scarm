package com.vivi.scarm

import shapeless._

trait Subset[A,SET<:HList]

trait LowerPrioritySubset {
  //conflicst with hnilIsSubset
  implicit def containedInTail[A,HD,TAIL<:HList]
    (implicit subset: Subset[A,TAIL]) = new Subset[A,HD::TAIL] {}

}

object Subset extends LowerPrioritySubset {
  def apply[A,SET<:HList](implicit subset: Subset[A,SET]) = subset

  implicit def hnilIsSubset[SET<:HList] = new Subset[HNil,SET] {}

  implicit def headAndTail[A,TAIL<:HList,SET<:HList]
    (implicit hd: Subset[A,SET], tl: Subset[TAIL,SET]) = new Subset[A::TAIL,SET] {}

  implicit def containedInHead[A,TAIL<:HList] = new Subset[A,A::TAIL] {}
}
