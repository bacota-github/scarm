package com.vivi.scarm

import scala.reflect.runtime.universe.{MethodSymbol,Type,TypeTag, typeOf}
import scala.reflect.runtime.universe.{Symbol => ReflectionSymbol}

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType


case class FieldMap[A](fields: Seq[FieldMap.Item]) {

  def withPrimaryKey: FieldMap[A] = copy(fields = fields.map(item =>
    if (isPrimaryKeyField(item)) item.copy(isPrimary=true) else item
  ))

  lazy val mapping: Map[Seq[String],FieldMap.Item] =
    fields.map(item=> (item.names, item) ).toMap
  def ++[B<:HList](other: FieldMap[B]):FieldMap[A::B] = FieldMap.concat(this,other)

  def firstFieldName = fields.head.names.head

  def isOptional(field: Seq[String]): Boolean =
    mapping.get(field).map(_.optional).getOrElse(false)

  def isPrimaryKeyField(field: FieldMap.Item) = field.names.head == firstFieldName

  def names(config: ScarmConfig) = fields.map(_.name(config))
  def prefixedNames(config: ScarmConfig) = fields.map(_.name(config.copy(prefixPrimaryKey=true)))

  def primaryKey[K](implicit witness: PrimaryKey[K,A]) =
    FieldMap[K](fields.takeWhile(_.isPrimary))

  def keyMap[K](kmap: FieldMap[K])(implicit witness: Subset[K,A]) = FieldMap[K](
    kmap.fields.map(kfield => this.fields.find(_.names == kfield.names).get)
  )

  def typeOf(field: Seq[String]): Option[Type] = mapping.get(field).map(_.tpe)

  override def toString = fields.toString
}


object FieldMap {

  case class Item(names: Seq[String], tpe: Type, optional: Boolean, isAnyVal: Boolean, isPrimary: Boolean) {

    def name(config: ScarmConfig) = {
      var use = names
      if (isAnyVal && !config.suffixAnyVal && use.length > 1)
        use = use.dropRight(1)
      if (isPrimary &&  !config.prefixPrimaryKey && use.length > 1)
        use = use.drop(1)
      if (config.snakeCase)
        use = use.map(snakeCase(_))
      use.mkString(config.fieldNameSeparator)
    }
  }

  private lazy val snakeCaseRegex = """([A-Z])""".r
  def snakeCase(name: String): String =
    snakeCaseRegex.replaceAllIn(name, "_" + _.matched.toLowerCase())

  implicit def apply[A](implicit ttag: TypeTag[A]): FieldMap[A] = FieldMap[A](
    fieldsFromType(Seq(), ttag.tpe, false, ttag.tpe <:< typeOf[AnyVal])
  )

  private def isCaseMethod(s: ReflectionSymbol): Boolean = 
    s.isMethod && s.asMethod.isCaseAccessor

  private def isCaseClass(tpe: Type): Boolean =
    tpe.members.exists(isCaseMethod(_))

  private def fieldsFromType(pre: Seq[String], tpe: Type, opt: Boolean, anyVal: Boolean): Seq[Item] =   {
    val members = tpe.members.sorted.collect {
      case s if isCaseMethod(s) => {
        val m = s.asMethod
        val names = pre :+ m.name.toString
        if (m.returnType <:< typeOf[Option[Any]])
          fieldsFromType(names, m.returnType.typeArgs.head, true, false)
        else if (isCaseClass(m.returnType)) 
          fieldsFromType(names, m.returnType, opt, m.returnType <:< typeOf[AnyVal])
        else Seq(Item(names, m.returnType, opt, anyVal, false))
      }
    }
    if (!members.isEmpty)
      members.flatten
    else
      Seq(Item(pre, tpe, opt, anyVal, false))
  }


  private[scarm] def concat[A,B<:HList](l: FieldMap[A], r: FieldMap[B]) =
    FieldMap[A :: B](l.fields ++ r.fields)
}

