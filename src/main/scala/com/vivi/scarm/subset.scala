package com.vivi.scarm

import shapeless._

trait Subset[A,SET]

trait LowerPrioritySubset {
  implicit def containedInTail[A,HD,TAIL<:HList]
    (implicit subset: Subset[A,TAIL]) = new Subset[A,HD::TAIL] {}
}

object Subset extends LowerPrioritySubset {
  def apply[A,SET](implicit subset: Subset[A,SET]) = subset

  implicit def hnilIsSubset[SET<:HList] = new Subset[HNil,SET] {}

  implicit def headAndTail[A,TAIL<:HList,SET<:HList]
    (implicit hd: Subset[A,SET], tl: Subset[TAIL,SET]) = new Subset[A::TAIL,SET] {}

  implicit def containedInHead[A,TAIL<:HList] = new Subset[A,A::TAIL] {}

  implicit def convertToHLists[TO,TREPR<:HList,FROM,FREPR<:HList](implicit
    toGen: LabelledGeneric.Aux[TO,TREPR],
    fromGen: LabelledGeneric.Aux[FROM,FREPR],
    subset: Subset[TREPR,FREPR]
  ) = new Subset[TO,FROM] {}
}



trait StructurallyEqual[A,B]

object  StructurallyEqual{
  def apply[A,B](implicit same: StructurallyEqual[A,B]) = same

  implicit def convertToHLists[A,ARepr<:HList,B,BRepr<:HList](implicit
    aGen: LabelledGeneric.Aux[A,ARepr],
    bGen: LabelledGeneric.Aux[B,BRepr],
    equality: StructurallyEqual[ARepr,BRepr]
  ) = new StructurallyEqual[A,B] {}

  implicit def reflexivity[A] = new StructurallyEqual[A,A] {}

  implicit def headsAreEqual[HD,TL1<:HList,TL2<:HList]
  (implicit tailsAreEqual: StructurallyEqual[TL1,TL2]) =
    new StructurallyEqual[HD::TL1, HD::TL2] {}
}




trait HeadIsStructurallyEqual[A,B]

object HeadIsStructurallyEqual {
  def apply[A,B](implicit same: HeadIsStructurallyEqual[A,B]) = same

  implicit def headIsStructurallyEqual[HD,TL<:HList,B](implicit
    equality: StructurallyEqual[HD,B]
  ) = new HeadIsStructurallyEqual[HD::TL,B] {}

  implicit def isStructurallyEqual[A,ARepr<:HList,B](implicit
    agen: Generic.Aux[A, ARepr],
    equality: HeadIsStructurallyEqual[ARepr,B]
  ) = new HeadIsStructurallyEqual[A,B] {}
}

