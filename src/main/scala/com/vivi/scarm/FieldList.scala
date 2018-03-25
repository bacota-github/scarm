package com.vivi.scarm

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.Witness
import shapeless.labelled.FieldType

/**
  * Based on Mike Limansky's blog post at
  * http://limansky.me/posts/2017-02-02-generating-sql-queries-with-shapeless.html
  */

trait FieldList[A] {
  val names: List[String]
}

trait FieldListLowPriority {
  implicit def primitiveFieldList[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    tLister: FieldList[T]
  ): FieldList[FieldType[K, H] :: T] =
    new FieldList[FieldType[K, H] ::T] {
      override val names = witness.value.name :: tLister.names
    }
}

object FieldList extends FieldListLowPriority {

  def apply[T](implicit fieldLister: FieldList[T]): FieldList[T] =
    fieldLister

  implicit def make[A,ARepr<:HList](
    implicit gen: LabelledGeneric.Aux[A, ARepr],
    generator: FieldList[ARepr]
  ): FieldList[A] = new FieldList[A] {
    override val names = generator.names
  }

  def genericLister[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    lister: Lazy[FieldList[R]]
  ): FieldList[A] = new FieldList[A] {
    override val names = lister.value.names
  }

  implicit val hnilLister: FieldList[HNil] = new FieldList[HNil] {
    override val names = Nil
  }

  implicit def hconsLister[K, H, T <: HList](implicit
    hLister: Lazy[FieldList[H]],
    tLister: FieldList[T]
  ): FieldList[FieldType[K, H] :: T] =
    new FieldList[FieldType[K, H] :: T] {
      override val names = hLister.value.names ++ tLister.names
    }
}

