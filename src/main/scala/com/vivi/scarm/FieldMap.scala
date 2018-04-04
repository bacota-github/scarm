package com.vivi.scarm

import scala.language.implicitConversions

import scala.reflect.runtime.universe.{Type,TypeTag}

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType


trait FieldMap[A] {
  val fields: Seq[FieldMap.Item]

  lazy val mapping: Map[String,(Type,Boolean)] = fields.map(_.entry).toMap
  def names = fields.map(_.name)
  def typeOf(field: String): Option[Type] = mapping.get(field).map(_._1)
  def isOptional(field: String): Boolean =
    mapping.get(field).map(_._2).getOrElse(false)

  def ++[B<:HList](other: FieldMap[B]):FieldMap[A::B] = FieldMap.concat(this,other)

  def prefix(pre: String, fields: Seq[FieldMap.Item]) =
    fields.map(it => it.copy(name=pre+"_"+it.name))
}

trait PrimitiveFieldMap {
  implicit def primitiveFieldMap[K <: Symbol, V](implicit
      k: Witness.Aux[K],
      typeTag: TypeTag[V]
  ) = new FieldMap[FieldType[K, V]] {
    override val fields =
      prefix("p", Seq( FieldMap.Item(k.value.name, typeTag.tpe, false) ))
  }
}


object FieldMap extends PrimitiveFieldMap {

  case class Item(name: String, tpe: Type, optional: Boolean) {
    def entry = (name, (tpe, optional))
  }

  def apply[A](implicit fmap: FieldMap[A]): FieldMap[A] = fmap


  implicit def genericFieldMap[A,ARepr<:HList](implicit
    generic: LabelledGeneric.Aux[A,ARepr],
    fieldMap: FieldMap[ARepr]
  ) = new FieldMap[A] {
    override val fields = prefix("gen", fieldMap.fields)
  }

  implicit def hnilFieldMap = new FieldMap[HNil] {
    override val fields = Seq()
  }

  implicit def recursiveFieldMap[K<:Symbol,H,HRepr<:HList](implicit
    k: Witness.Aux[K],
    generic: LabelledGeneric.Aux[H,HRepr],
    hmap: Lazy[FieldMap[HRepr]]
  ) = new FieldMap[FieldType[K,H]] {
    override val fields = prefix("recurse", hmap.value.fields)
  }

  implicit def hconsFieldMap[K<:Symbol, H, T<:HList](implicit
    k: Witness.Aux[K],
    hmap: FieldMap[FieldType[K,H]],
    tmap: FieldMap[T]
  ) = new FieldMap[FieldType[K,H] :: T] {
    override val fields = prefix("hcons", hmap.fields ++ tmap.fields)
  }

  implicit def optionalFieldMap[K<:Symbol, V](implicit
    from: FieldMap[FieldType[K, V]]
  ) =  new FieldMap[FieldType[K,Option[V]]] {
    override val fields = prefix("opt", from.fields.map(_.copy(optional=true)))
  }

  private[scarm] def prefix(pre: String, from: Seq[Item]) =
    from.map(it => it.copy(name=pre+it.name))

  private[scarm] def concat[A,B<:HList](l: FieldMap[A], r: FieldMap[B]) =
    new FieldMap[A :: B] {
      override val fields = l.fields ++ r.fields
    }

}

