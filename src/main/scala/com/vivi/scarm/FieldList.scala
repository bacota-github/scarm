package com.vivi.scarm

import scala.reflect.runtime.universe.{Type,TypeTag}
import shapeless.{ ::, HList, HNil, Lazy, LabelledGeneric, Witness }
import shapeless.labelled.FieldType

trait FieldList[A] {
  val names: List[String]
  def prefix(pre: String) = FieldList.prefix[A](pre+"_", this)
}

trait LowPriorityFieldList {
  implicit def primitiveFieldList[T]= 
    new FieldList[T] {
      override val names = List("")
      override def prefix(pre: String) = FieldList.prefix[T](pre, this)
    }
}

object FieldList extends LowPriorityFieldList {

  def apply[T](implicit tlist: FieldList[T]): FieldList[T] = tlist

  implicit def productFieldList[P, Repr<:HList](implicit
    generic: LabelledGeneric.Aux[P,Repr],
    flist: FieldList[Repr]
  ) = new FieldList[P] { override val names = flist.names }

  implicit val hnilFieldList = new FieldList[HNil] { override val names = List() }

  implicit def hconsFieldList[K<:Symbol, H, T<:HList](implicit
    k: Witness.Aux[K],
    hlist: Lazy[FieldList[H]],
    tlist: FieldList[T]
  ) = new FieldList[H :: T] {
    override val names = hlist.value.prefix(k.value.name).names ++ tlist.names
  }

  implicit def optionalFieldList[T](implicit tlist: FieldList[T]) =
    new FieldList [Option[T]] { override val names = tlist.names }

  private[scarm] def prefix[A](pre: String, flist: FieldList[A]): FieldList[A] =
    new FieldList[A] { override val names =  flist.names.map(pre+_) }

}
