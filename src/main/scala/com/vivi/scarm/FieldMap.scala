package com.vivi.scarm

import scala.language.implicitConversions

import scala.reflect.runtime.universe.{MethodSymbol,Type,TypeTag, typeOf}
import scala.reflect.runtime.universe.{Symbol => ReflectionSymbol}

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType


trait FieldMap[A] {
  val fields: Seq[FieldMap.Item]

  lazy val mapping: Map[String,FieldMap.Item] =
    fields.map(item=> (item.name, item) ).toMap
  def ++[B<:HList](other: FieldMap[B]):FieldMap[A::B] = FieldMap.concat(this,other)
  lazy val autoGenFields: Seq[String] = fields.filter(_.autogen).map(_.name)
  def isOptional(field: String): Boolean =
    mapping.get(field).map(_.optional).getOrElse(false)
  def names = fields.map(_.name)
  def prefix(pre: String) = FieldMap.prefix[A](pre+"_", this)
  def typeOf(field: String): Option[Type] = mapping.get(field).map(_.tpe)

  override def toString = fields.toString
}


object FieldMap {

  case class Item(name: String, tpe: Type, optional: Boolean, autogen:Boolean)

  implicit def apply[A](implicit ttag: TypeTag[A]): FieldMap[A] = new FieldMap[A]{
    override val fields = fieldsFromType("", ttag.tpe, false, false)
  }

  private def isCaseMethod(s: ReflectionSymbol): Boolean = 
    s.isMethod && s.asMethod.isCaseAccessor

  private def isCaseClass(tpe: Type): Boolean =
    tpe.members.exists(isCaseMethod(_))


  private def fieldsFromType(pre: String, tpe: Type, opt: Boolean, autogen: Boolean): Seq[Item] =   {
    val members = tpe.members.sorted.collect {
      case s if isCaseMethod(s) => {
        val m = s.asMethod
        val name = (if (pre == "") "" else pre+"_") + m.name
        if (m.returnType <:< typeOf[Option[Any]])
          fieldsFromType(name, m.returnType.typeArgs.head, true, false)
        else if (m.returnType <:< typeOf[Autogen[Any]])
          fieldsFromType(name, m.returnType.typeArgs.head, opt, true)
        else if (isCaseClass(m.returnType)) {
          val prefix = if (tpe <:< typeOf[Entity[Any]] && m.name.toString == "id") pre else name
          fieldsFromType(prefix, m.returnType, opt, false)
        }
        else Seq(Item(name, m.returnType, opt, false))
      }
    }
    if (!members.isEmpty)
      members.flatten
    else
      Seq(Item(pre, tpe, opt, false))
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

