package com.vivi.scarm
import shapeless._
import shapeless.labelled._

trait Subset[A,SET]

trait LowestPrioritySubset {
  implicit def ignoreOption[A,SET](implicit s: Subset[A,SET]) =
    new Subset[Option[A],SET] {}

  implicit def headIsSubset[A,HD,TAIL<:HList](implicit s: Subset[A,HD]) =
    new Subset[A, HD::TAIL] {}
}

trait LowerPrioritySubset extends LowestPrioritySubset {
  implicit def subsetOfTail[A,HD,TAIL<:HList]
    (implicit subset: Subset[A,TAIL]) = new Subset[A,HD::TAIL] {}

  implicit def fieldTypes[K <: Symbol, A, B](implicit isSubset: Lazy[Subset[A,B]]) =
    new Subset[FieldType[K,A],FieldType[K,B]] {}
}


object Subset extends LowerPrioritySubset {
  def apply[A,SET](implicit subset: Subset[A,SET]) = subset

  implicit def hnilIsSubset[SET<:HList] = new Subset[HNil,SET] {}

  implicit def isHead[A,TAIL<:HList] = new Subset[A, A::TAIL] {}

  implicit def headAndTail[HD,TAIL<:HList,SET<:HList](implicit
    hd: Subset[HD,SET],
    tl: Subset[TAIL,SET]
  ) = new Subset[HD::TAIL,SET] {}

  implicit def convertToHLists[A,AREPR<:HList,B,BREPR<:HList](implicit
    aGen: LabelledGeneric.Aux[A,AREPR],
    bGen: LabelledGeneric.Aux[B,BREPR],
    subset: Subset[AREPR,BREPR]
  ) = new Subset[A,B] {}
}


trait EqualStructure[A,B]

trait LowPriorityEqualStructure  extends{
  implicit def reflexive[A] = new EqualStructure[A,A] {}
}

object EqualStructure extends LowPriorityEqualStructure {
  def apply[A,B](implicit eq: EqualStructure[A,B]) = eq

  implicit def similarHead[HD1, TAIL1<:HList, HD2, TAIL2<:HList](implicit
    similarHead: EqualStructure[HD1, HD2],
    similarTail: EqualStructure[TAIL1,TAIL2]
    ) = new EqualStructure[HD1::TAIL1, HD2::TAIL2] {}

  implicit def caseClass[A,Repr,B](implicit
    gen: Generic.Aux[A,Repr],
    similarity: EqualStructure[Repr, B::HNil]
  ) = new EqualStructure[A, B] {}
}



trait SimilarStructure[A,B]

trait LowPrioritySimilarStructure  extends{
  implicit def reflexive[A] = new SimilarStructure[A,A] {}
  implicit def option[A] = new SimilarStructure[Option[A],A] {}
}

object SimilarStructure extends LowPrioritySimilarStructure {
  def apply[A,B](implicit eq: SimilarStructure[A,B]) = eq

  implicit def similarHead[HD1, TAIL1<:HList, HD2, TAIL2<:HList](implicit
    similarHead: SimilarStructure[HD1, HD2],
    similarTail: SimilarStructure[TAIL1,TAIL2]
    ) = new SimilarStructure[HD1::TAIL1, HD2::TAIL2] {}

  implicit def caseClass[A,Repr,B](implicit
    gen: Generic.Aux[A,Repr],
    similarity: SimilarStructure[Repr, B::HNil]
  ) = new SimilarStructure[A, B] {}
}
