package com.vivi.scarm

import shapeless._
import shapeless.labelled._


trait Subset[A,SET]

trait LowestPrioritySubset {
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



trait StructurallyEqual[A,B]

object  StructurallyEqual {
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



trait StructurallySimilar[A,B]

object StructurallySimilar  {
  def apply[A,B](implicit similar: StructurallySimilar[A,B]) = similar


  implicit def convertToHLists[A,ARepr<:HList,B,BRepr<:HList](implicit
    aGen: LabelledGeneric.Aux[A,ARepr],
    bGen: LabelledGeneric.Aux[B,BRepr],
    equality: StructurallySimilar[ARepr,BRepr]
  ) = new StructurallySimilar[A,B] {}

  implicit def reflexivity[A] = new StructurallySimilar[A,A] {}

  implicit def similarity[A] = new StructurallySimilar[Option[A] ,A] {}

  implicit def headsAreEqual[HD,TL1<:HList,TL2<:HList]
  (implicit tailsAreSimilar: StructurallySimilar[TL1,TL2]) =
    new StructurallySimilar[HD::TL1, HD::TL2] {}

  implicit def headsAreSimilar[HD,TL1<:HList,TL2<:HList]
  (implicit tailsAreSimilar: StructurallySimilar[TL1,TL2]) =
    new StructurallySimilar[Option[HD]::TL1, HD::TL2] {}
}


trait HeadIsStructurallySimilar[A,B]

object HeadIsStructurallySimilar {
  def apply[A,B](implicit same: HeadIsStructurallySimilar[A,B]) = same

  implicit def headIsStructurallySimilar[HD,TL<:HList,B](implicit
    equality: StructurallySimilar[HD,B]
  ) = new HeadIsStructurallySimilar[HD::TL,B] {}

  implicit def isStructurallySimilar[A,ARepr<:HList,B](implicit
    agen: Generic.Aux[A, ARepr],
    equality: HeadIsStructurallySimilar[ARepr,B]
  ) = new HeadIsStructurallySimilar[A,B] {}
}

