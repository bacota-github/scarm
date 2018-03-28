package com.vivi.scarm

import scala.language.implicitConversions

import scala.reflect.runtime.universe.{Type,TypeTag}

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.Witness
import shapeless.labelled.FieldType

/**
  * Based on Mike Limansky's blog post at
  * http://limansky.me/posts/2017-02-02-generating-sql-queries-with-shapeless.html
  */

trait FieldMap[A] {
  //Maps field name to field type and nullability
  val mapping: Map[String,(Type, Boolean)] 
}

trait FieldMapLowPriority {
  implicit def primitiveFieldMap[K <: Symbol, H, T <: HList]
  (implicit
    witness: Witness.Aux[K],
    tMap: FieldMap[T],
    typeTag: TypeTag[H]
  ): FieldMap[FieldType[K, H] :: T] =
    new FieldMap[FieldType[K, H] ::T] {
      override val mapping =
        tMap.mapping + (witness.value.name.toString -> (typeTag.tpe, false))
    }
}


object FieldMap extends FieldMapLowPriority {

  def apply[T](implicit fieldMap: FieldMap[T]): FieldMap[T] = fieldMap

  implicit def make[A,ARepr<:HList](
    implicit gen: LabelledGeneric.Aux[A, ARepr],
    generator: FieldMap[ARepr]
  ): FieldMap[A] = new FieldMap[A] {
    override val mapping = generator.mapping
  }

  implicit val hnilMap: FieldMap[HNil] = new FieldMap[HNil] {
    override val mapping = Map()
  }

  implicit def hconsMap[K, H, T <: HList](implicit
    hMap: Lazy[FieldMap[H]],
    tMap: FieldMap[T]
  ): FieldMap[FieldType[K, H] :: T] =
    new FieldMap[FieldType[K, H] :: T] {
      override val mapping = hMap.value.mapping ++ tMap.mapping
    }

}

