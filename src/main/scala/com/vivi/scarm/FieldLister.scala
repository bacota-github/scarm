package com.vivi.scarm

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.Witness
import shapeless.labelled.FieldType

/**
  * Based on Mike Limansky's blog post at
  * http://limansky.me/posts/2017-02-02-generating-sql-queries-with-shapeless.html
  */

trait FieldLister[A] {
  val names: List[String]
}

trait FieldListerLowPriority {
  implicit def primitiveFieldLister[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    tLister: FieldLister[T]
  ): FieldLister[FieldType[K, H] :: T] =
    new FieldLister[FieldType[K, H] ::T] {
      override val names = witness.value.name :: tLister.names
    }
}

object FieldLister extends FieldListerLowPriority {

  def apply[T](implicit fieldLister: FieldLister[T]): FieldLister[T] =
    fieldLister


  implicit def make[A,ARepr<:HList](
    implicit gen: LabelledGeneric.Aux[A, ARepr],
    generator: FieldLister[ARepr]
  ): FieldLister[A] = new FieldLister[A] {
    override val names = generator.names
  }

  def genericLister[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    lister: Lazy[FieldLister[R]]
  ): FieldLister[A] = new FieldLister[A] {
    override val names = lister.value.names
  }

  implicit val hnilLister: FieldLister[HNil] = new FieldLister[HNil] {
    override val names = Nil
  }

  implicit def hconsLister[K, H, T <: HList](implicit
    hLister: Lazy[FieldLister[H]],
    tLister: FieldLister[T]
  ): FieldLister[FieldType[K, H] :: T] =
    new FieldLister[FieldType[K, H] :: T] {
      override val names = hLister.value.names ++ tLister.names
    }
}

