package com.vivi.scarm

import shapeless._

trait Subset[A,SET]

trait LowerPrioritySubset {
  //conflicst with hnilIsSubset
  implicit def containedInTail[A,HD,TAIL<:HList]
    (implicit subset: Subset[A,TAIL]) = new Subset[A,HD::TAIL] {}

}

object Subset extends LowerPrioritySubset {
  def apply[A,SET](implicit subset: Subset[A,SET]) = subset

  implicit def hnilIsSubset[SET<:HList] = new Subset[HNil,SET] {}

  implicit def headAndTail[A,TAIL<:HList,SET<:HList]
    (implicit hd: Subset[A,SET], tl: Subset[TAIL,SET]) = new Subset[A::TAIL,SET] {}

  implicit def containedInHead[A,TAIL<:HList] = new Subset[A,A::TAIL] {}

  implicit def project[TO,TREPR<:HList,FROM,FREPR<:HList](implicit
    toGen: LabelledGeneric.Aux[TO,TREPR],
    fromGen: LabelledGeneric.Aux[FROM,FREPR],
    subset: Subset[TREPR,FREPR]
  ) = new Subset[TO,FROM] {}
}



/*
trait StructuralEquality[A<:HList,B<:HList]

object  StructuralEquality{
  def apply[A<:HList,B<:HList](implicit same: StructuralEquality[A,B]) = same

  implicit def hnilsAreEqual = StructuralEquality[HNil,HNil] {}

  implicit def headsAreEqual[HD,TL1<:HList,TL2<:HList]
  (implicit tailsAreEqual: StructuralEquality[TL1,TL2]) =
    StructuralEquality[HD::TL1, HD::TL] {}
}

trait StructurallyEqual[A,B]

object StructurallyEqual {
  def apply[A,B](implicit same: StructuralllyEqual[A,B]) = same

  implicit def equalHLists[A,ARepr,B,BRepr](
    implicit aGen: LabelledGeneric.Aux[A,ARepr],
    implicit aGen: LabelledGeneric.Aux[A,ARepr],
      equality: StructuralEquality[A,B]
  ) =
    StructuralEquality[A,B] {}
}
 */
