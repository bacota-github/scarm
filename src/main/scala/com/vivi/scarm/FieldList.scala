package com.vivi.scarm

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.Witness
import shapeless.labelled.FieldType

/**
  * Loosely ased on Mike Limansky's blog post at
  * http://limansky.me/posts/2017-02-02-generating-sql-queries-with-shapeless.html
  */

trait FieldList[A] {
  val names: List[String]
}


trait PrimitiveFieldList {
  implicit def primitiveFieldList[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    tList: FieldList[T]
  ) = new FieldList[FieldType[K, H] ::T] {
      override val names = "primitive_"+witness.value.name :: tList.names
    }
}

trait OptionFieldList extends PrimitiveFieldList {
  implicit def optionFieldList[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hList: Lazy[FieldList[H]],
    tList: FieldList[T]
  ) =  new FieldList[FieldType[K, Option[H]] ::T] {
      override val names =
        FieldList.prefix("option_"+witness.value.name, hList.value) ++ tList.names
  }
}

object FieldList extends OptionFieldList {

  def apply[T](implicit fieldList: FieldList[T]): FieldList[T] = fieldList

  implicit def makeFieldList[A,ARepr<:HList](implicit
    gen: LabelledGeneric.Aux[A, ARepr],
    generator: FieldList[ARepr]
  ) = new FieldList[A] {
    override val names = generator.names.map("make_" + _)
  }


  implicit val hnilFieldList: FieldList[HNil] = new FieldList[HNil] {
    override val names = Nil
  }

  implicit def hconsFieldList[K<:Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hList: Lazy[FieldList[H]],
    tList: FieldList[T]
  ): FieldList[FieldType[K,H] ::T]= new FieldList[FieldType[K, H] :: T] {
      override val names = 
        prefix("hcons_"+witness.value.name, hList.value) ++ tList.names
    }


  private[scarm] def prefix[A](prefix: String, list: FieldList[A]): List[String] =
    if (prefix == "id") list.names else list.names.map(prefix + "_" + _)
}

