package com.vivi.scarm
import shapeless._
import shapeless.labelled._

import scala.language.implicitConversions

import java.time._


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

  implicit def optionTypes[K <: Symbol, A] =
    new Subset[FieldType[K,Option[A]],FieldType[K,A]] {}
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



trait Catenation[A<:HList,B<:HList,C<:HList]

trait LowerPriorityCatenation {

  implicit def hnilConcat[A<:HList] = new Catenation[HNil, A, A] {}

  implicit def concat[HD, TL<:HList, B<:HList, C<:HList](implicit
    tailCat: Catenation[TL,B,C]
  ) = new Catenation[HD::TL, B,  HD :: C] {}
}

object Catenation extends LowerPriorityCatenation {
  def apply[A<:HList,B<:HList,C<:HList](implicit cat: Catenation[A,B,C]) = cat

  implicit def concatHNil[A<:HList] = new Catenation[A, HNil, A] {}
}


trait Flattened[A,FLAT]

trait LowPriorityFlattened {
  implicit def flathead[HD,TAIL<:HList,FTAIL<:HList](implicit
    flatTail: Flattened[TAIL,FTAIL]
  ): Flattened[HD::TAIL, HD::FTAIL] = new Flattened[HD::TAIL, HD::FTAIL] {}

  implicit def optionHead[HD,TAIL<:HList,FTAIL<:HList](implicit
    flatTail: Flattened[TAIL,FTAIL]
  ): Flattened[Option[HD]::TAIL, HD::FTAIL] = new Flattened[Option[HD]::TAIL, HD::FTAIL] {}

  implicit def transitive[A,B,C](implicit
    ab: Flattened[A,B],
    bc: Lazy[Flattened[B,C]]
  ): Flattened[A,C] = new Flattened[A,C] {}
}

object Flattened extends LowPriorityFlattened { 
  def apply[A,FLAT](implicit flat: Flattened[A,FLAT]) = flat

  implicit def reflexive[A]: Flattened[A,A] = new Flattened[A,A] {}

  implicit def flattenedCaseClass[A,Repr<:HList,FLAT<:HList](implicit
    gen: Generic.Aux[A,Repr],
    flat: Flattened[Repr,FLAT]
  ): Flattened[A,FLAT] = new Flattened[A,FLAT] {}

  implicit def flattenedHList[HD,FHD<:HList,TAIL<:HList,FTAIL<:HList,FLAT<:HList](implicit
    flatHead: Flattened[HD,FHD],
    flatTail: Flattened[TAIL,FTAIL],
    cat: Catenation[FHD,FTAIL,FLAT]
  ): Flattened[HD::TAIL, FLAT]  = new Flattened[HD::TAIL, FLAT] {}
}




trait StructurallyEqual[A,B]

trait LowestPriorityStructurallyEqual {
  implicit def convertLeftToHLists[A,AREPR<:HList,B](implicit
    aGen: Generic.Aux[A,AREPR],
    eq: StructurallyEqual[AREPR,B]
  ) = new StructurallyEqual[A,B] {}

  implicit def convertRightToHLists[A,B,BREPR<:HList](implicit
    aGen: Generic.Aux[B,BREPR],
    eq: StructurallyEqual[A,BREPR]
  ) = new StructurallyEqual[A,B] {}
}

trait LowerPriorityStructurallyEqual extends LowestPriorityStructurallyEqual {

  implicit def headAndTail[HD,TAIL<:HList,HD2, TAIL2<:HList](implicit
    hd: StructurallyEqual[HD,HD2],
    tl: StructurallyEqual[TAIL,TAIL2]
  ) = new StructurallyEqual[HD::TAIL, HD::TAIL2] {}

  implicit def convertToHLists[A,AREPR<:HList,B,BREPR<:HList](implicit
    aGen: Generic.Aux[A,AREPR],
    bGen: Generic.Aux[B,BREPR],
    eq: StructurallyEqual[AREPR,BREPR]
  ) = new StructurallyEqual[A,B] {}
}

object StructurallyEqual {
  def apply[A,SET](implicit eq: StructurallyEqual[A,SET]) = eq

  implicit def reflexive[A] = new StructurallyEqual[A,A] {}

  implicit def equivalentlyFlat[A,B,FLAT](implicit
    aflat: Flattened[A,FLAT],
    bflat: Flattened[B,FLAT]
  ) = new StructurallyEqual[A,B] {}
}

