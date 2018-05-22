package com.vivi.scarm

import scala.language.implicitConversions

import scala.reflect.runtime.universe.{MethodSymbol,Type,TypeTag, typeOf}
import scala.reflect.runtime.universe.{Symbol => ReflectionSymbol}

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType


case class FieldMap[A](firstFieldName: String, fields: Seq[FieldMap.Item]) {

  lazy val mapping: Map[String,FieldMap.Item] =
    fields.map(item=> (item.name, item) ).toMap
  def ++[B<:HList](other: FieldMap[B]):FieldMap[A::B] = FieldMap.concat(this,other)
  def isOptional(field: String): Boolean =
    mapping.get(field).map(_.optional).getOrElse(false)
  def names = fields.map(_.name)
  def prefix(pre: String) = FieldMap.prefix[A](pre+"_", this)
  def typeOf(field: String): Option[Type] = mapping.get(field).map(_.tpe)

  override def toString = fields.toString
}


object FieldMap {

  case class Item(name: String, tpe: Type, optional: Boolean)

  implicit def apply[A](implicit ttag: TypeTag[A]): FieldMap[A] = FieldMap[A](
    firstFieldNameOfType(ttag.tpe),
    fieldsFromType("", ttag.tpe, false)
  )

  private def firstFieldNameOfType(tpe: Type): String = 
    tpe.members.sorted.find(isCaseMethod(_)) match {
      case None => ""
      case Some(m) => m.name.toString()
    }

  private def isCaseMethod(s: ReflectionSymbol): Boolean = 
    s.isMethod && s.asMethod.isCaseAccessor

  private def isCaseClass(tpe: Type): Boolean =
    tpe.members.exists(isCaseMethod(_))

  private def fieldsFromType(pre: String, tpe: Type, opt: Boolean): Seq[Item] =   {
    val members = tpe.members.sorted.collect {
      case s if isCaseMethod(s) => {
        val m = s.asMethod
        lazy val name = (if (pre == "") "" else pre+"_") + m.name
        if (m.returnType <:< typeOf[Option[Any]])
          fieldsFromType(name, m.returnType.typeArgs.head, true)
        else if (isCaseClass(m.returnType)) 
          fieldsFromType(name, m.returnType, opt)
        else Seq(Item(name, m.returnType, opt))
      }
    }
    if (!members.isEmpty)
      members.flatten
    else
      Seq(Item(pre, tpe, opt))
  }

  private[scarm] def prefix[A](pre: String, from: FieldMap[A]): FieldMap[A] =
    FieldMap[A](pre+from.firstFieldName,
      from.fields.map(it => it.copy(name=pre+it.name))
    )

  private[scarm] def concat[A,B<:HList](l: FieldMap[A], r: FieldMap[B]) =
    FieldMap[A :: B](l.firstFieldName, l.fields ++ r.fields)
}

