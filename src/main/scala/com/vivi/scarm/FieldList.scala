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
    tList: FieldList[T]
  ): FieldList[FieldType[K, H] :: T] =
    new FieldList[FieldType[K, H] ::T] {
      override val names = witness.value.name :: tList.names
    }
}

object FieldList extends FieldListLowPriority {

  def apply[T](implicit fieldList: FieldList[T]): FieldList[T] =
    fieldList

  implicit def make[A,ARepr<:HList](
    implicit gen: LabelledGeneric.Aux[A, ARepr],
    generator: FieldList[ARepr]
  ): FieldList[A] = new FieldList[A] {
    override val names = generator.names
  }

  implicit val hnilList: FieldList[HNil] = new FieldList[HNil] {
    override val names = Nil
  }

  implicit def hconsList[K<:Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hList: Lazy[FieldList[H]],
    tList: FieldList[T]
  ): FieldList[FieldType[K, H] :: T] =
    new FieldList[FieldType[K, H] :: T] {
      override val names = {
        val wname = witness.value.name
        val hnames = hList.value.names
        val mappedNames =
          if (wname == "id") hnames
          else hnames.map(name => witness.value.name + "_" + name)
        mappedNames ++ tList.names
      }
    }
}

