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

  def prefix(pre: String) = FieldMap.prefix[A](pre+"_", this)
}

trait PrimitiveFieldMap {
  implicit def primitiveFieldMap[V](implicit  typeTag: TypeTag[V]) =
    new FieldMap[V] {
      override val fields = Seq( FieldMap.Item("", typeTag.tpe, false) )

      override def prefix(pre: String) = FieldMap.prefix[V](pre, this)
    }
}


trait keylessFieldMap extends PrimitiveFieldMap {
  implicit def keylessHconsFieldMap[H, T<:HList](implicit
    hmap: FieldMap[H],
    tmap: FieldMap[T]
  ) = new FieldMap[H :: T] {
    override val fields = hmap.fields ++ tmap.fields
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
    override val fields = fieldMap.fields
  }

  implicit def hnilFieldMap = new FieldMap[HNil] {
    override val fields = Seq()
  }
/*
  implicit def recursiveFieldMap[K<:Symbol,H,HRepr<:HList](implicit
    k: Witness.Aux[K],
    generic: LabelledGeneric.Aux[H,HRepr],
    hmap: Lazy[FieldMap[HRepr]]
  ) = new FieldMap[FieldType[K,H]] {
    override val fields = prefix("recurse", hmap.value.fields)
  }
 */
  implicit def hconsFieldMap[K<:Symbol, H, T<:HList](implicit
    k: Witness.Aux[K],
    hmap: FieldMap[H],
    tmap: FieldMap[T]
  ) = new FieldMap[FieldType[K,H] :: T] {
    override val fields = hmap.prefix(k.value.name).fields ++ tmap.fields
  }

  implicit def optionalFieldMap[K<:Symbol, V](implicit
    from: FieldMap[FieldType[K, V]]
  ) =  new FieldMap[FieldType[K,Option[V]]] {
    override val fields = from.fields.map(_.copy(optional=true))
  }

  private[scarm] def prefix[A](pre: String, from: FieldMap[A]): FieldMap[A] =
    new FieldMap[A] {
      override val fields = from.fields.map(it => it.copy(name=pre+it.name))
    }

  private[scarm] def concat[A,B<:HList](l: FieldMap[A], r: FieldMap[B]) =
    new FieldMap[A :: B] {
      override val fields = l.fields ++ r.fields
    }

}

