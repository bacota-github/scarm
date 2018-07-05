package com.vivi.scarm

import scala.language.implicitConversions

import scala.reflect.runtime.universe.{MethodSymbol,Type,TypeTag, typeOf}
import scala.reflect.runtime.universe.{Symbol => ReflectionSymbol}

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType


case class FieldMap[A](firstFieldName: String, fields: Seq[FieldMap.Item]) {

  lazy val mapping: Map[Seq[String],FieldMap.Item] =
    fields.map(item=> (item.names, item) ).toMap
  def ++[B<:HList](other: FieldMap[B]):FieldMap[A::B] = FieldMap.concat(this,other)
  def isOptional(field: Seq[String]): Boolean =
    mapping.get(field).map(_.optional).getOrElse(false)

  def names(config: ScarmConfig) = fields.map(_.name(config))

  def prefix(pre: String) = FieldMap(firstFieldName, 
    fields.map(item => item.copy(names = pre +: item.names))
  )

  def stripFirstFieldName = FieldMap[A](firstFieldName, fields.map(_.strip(firstFieldName)))

  def typeOf(field: Seq[String]): Option[Type] = mapping.get(field).map(_.tpe)

  override def toString = fields.toString
}


object FieldMap {

  case class Item(names: Seq[String], tpe: Type, optional: Boolean) {

    def name(config: ScarmConfig) = {
      val nms = if (config.snakeCase) names.map(snakeCase(_)) else names
      nms.mkString(config.fieldNameSeparator)
    }

    def strip(prefix: String) =
      copy(names =   if (names.head == prefix) names.tail else names)
  }

  private lazy val snakeCaseRegex = """([A-Z])""".r
  def snakeCase(name: String): String =
    snakeCaseRegex.replaceAllIn(name, "_" + _.matched.toLowerCase())

  implicit def apply[A](implicit ttag: TypeTag[A]): FieldMap[A] = FieldMap[A](
    firstFieldNameOfType(ttag.tpe),
    fieldsFromType(Seq(), ttag.tpe, false)
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

  private def fieldsFromType(pre: Seq[String], tpe: Type, opt: Boolean): Seq[Item] =   {
    val members = tpe.members.sorted.collect {
      case s if isCaseMethod(s) => {
        val m = s.asMethod
        val names = pre :+ m.name.toString
        if (m.returnType <:< typeOf[Option[Any]])
          fieldsFromType(names, m.returnType.typeArgs.head, true)
        else if (isCaseClass(m.returnType)) 
          fieldsFromType(names, m.returnType, opt)
        else Seq(Item(names, m.returnType, opt))
      }
    }
    if (!members.isEmpty)
      members.flatten
    else
      Seq(Item(pre, tpe, opt))
  }


  private[scarm] def concat[A,B<:HList](l: FieldMap[A], r: FieldMap[B]) =
    FieldMap[A :: B](l.firstFieldName, l.fields ++ r.fields)
}

