import shapeless.{ HList, HNil, LabelledGeneric, Witness }
import scala.reflect.runtime.universe.{TypeTag, typeOf}


/** Table: generic helper for getting field-names and insert statements.
  *
  * Doobie doesn't do anything to help creating INSERT and UPDATE
  * statements. This fills that gap using shapeless.
  *
  * First create an implicit instance for the case-class corresponding
  * to your database table:
  *
  *     implicit val myThingTable: Table[MyThing] =
  *         Table.forType[MyThing].create()
  *
  * You can now use that for inserts:
  *
  *     myThingTable.insert.run(myThingInst)
  * 
  * Or for selects:
  * 
  *     (sql"SELECT" ++ 
  *      myThingTable.selectFields("thingA") ++ 
  *      myThingTable.selectFields("thingB") ++ 
  *      sql"FROM my_thing AS a, my_thing AS b WHERE ...")
  *     .query[(MyThing, MyThing)].to[Vector]
  */
trait Table[T] {
  val fieldNames: Seq[String]
  val tableName: String

  def snakeCaseTransformation(name: String) = name

  def insert(data: T)(
      implicit composit: doobie.Composite[T]): doobie.Update0 = {
    val holes = Seq.fill(fieldNames.size)("?").mkString(",")
    val quotedFieldNames = fieldNames.map('"' + _ + '"').mkString(",")
    doobie
      .Update(
        s"""INSERT INTO "$tableName" ($quotedFieldNames) VALUES ($holes)""")
      .toUpdate0(data)
  }

  def selectFields(table: String): doobie.Fragment =
    doobie.Fragment.const0(
      fieldNames.map(f => s""""$table"."$f" AS "$table.$f"""").mkString(", "))
}

object Table {

  def snakeCaseTransformation(name: String) = name
  // conjure
  def apply[T](implicit dbTable: Table[T]): Table[T] = dbTable

  implicit val hnilTable: Table[HNil] = new Table[HNil] {
    override val fieldNames = Seq.empty[String]
    override val tableName = ""
  }

  implicit def hlistTable[K <: Symbol, H, T <: HList](
      implicit key: Witness.Aux[K],
      hTable: Table[H],
      tTable: Table[T]): Table[FieldType[K, H] :: T] =
    new Table[FieldType[K, H] :: T] {
      override val fieldNames = {
        val fieldName = snakeCaseTransformation(key.value.name)
        val hNames = hTable.fieldNames match {
          case Seq() => Seq(fieldName)
          case names => names.map(fieldName + "." + _)
        }
        hNames ++ tTable.fieldNames
      }
      override val tableName = ""
    }

  // use doobie.Meta[] as a proxy of all field types.
  implicit def metaDbField[A: doobie.Meta]: Table[A] = new Table[A] {
    override val fieldNames = Seq.empty[String]
    override val tableName = ""
  }

  // doobie doesn't define a Meta[Option[A]] (it instead defines
  // Composite[Option[A]]) so we need to manually create a
  // Table[Option[A]].
  implicit def optionDbField[A](
      implicit dbTable: Table[A]): Table[Option[A]] =
    new Table[Option[A]] {
      override val fieldNames = dbTable.fieldNames
      override val tableName = ""
    }

  case class GenericBuilder[A](tblName: String) {
    def create[ARepr <: HList]()(implicit gen: LabelledGeneric.Aux[A, ARepr],
                                 dbTable: Table[ARepr]): Table[A] = {
      new Table[A] {
        override val fieldNames = dbTable.fieldNames
        override val tableName = tblName
      }
    }
  }

  def forType[A: TypeTag](): GenericBuilder[A] =
    new GenericBuilder[A](
      snakeCaseTransformation(typeOf[A].typeSymbol.name.toString))

  def forType[A: TypeTag](tableName: String): GenericBuilder[A] =
    new GenericBuilder[A](tableName)
}
