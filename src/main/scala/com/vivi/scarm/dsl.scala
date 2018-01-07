package com.vivi.scarm

import doobie.util.update.Update0
import doobie.ConnectionIO
import fs2.Stream

import scala.util.{Failure,Success,Try}

sealed trait DatabaseObject {
  def create: Update0
  def drop: Update0
  def name: String
}

sealed trait Queryable[K, T] {
  def query(key: K): Stream[ConnectionIO,T]
}


case class Table[K,E](
  name: String,
  id: E => K
) extends DatabaseObject with Queryable[K,E] {
  override def create: Update0 = null
  override def drop: Update0 = null
  override def query(key: K): Stream[ConnectionIO,E] = Stream()
// def fetch(key: K): ConnectionIO[Option[E]] =  
//    query(key).head
  def create(enties: E*): Try[Unit] = Success(Unit)
  //def createReturning(entities: E*): Try[Seq[E]]
  def delete(keys: K*): Try[Unit] = Success(Unit)
  def save(enties: E*): Try[Unit] = Success(Unit)
  //def savveReturning(entities: E*): Try[Seq[E]]
  def update(enties: E*): Try[Unit] = Success(Unit)
  //def updateReturning(entities: E*): Try[Seq[E]]
}

case class View[K,E](
  name: String,
  id: E => K
) extends DatabaseObject with Queryable[K,E] {
  override def create: Update0 = null
  override def drop: Update0 = null 
  override def query(key: K): Stream[ConnectionIO,E] = Stream()
}

case class Index[K,E](
  name: String,
  table: Table[_,E],
  id: E => K
) extends DatabaseObject with Queryable[K,E] {
  override def create: Update0 = null
  override def drop: Update0 = null
  override def query(key: K): Stream[ConnectionIO,E] = Stream()
}


case class ForeignKey[FROM,PK,TO](
  name: String,
  from: Table[_,FROM],
  to: Table[PK,TO],
  fromKey: FROM => PK
) extends DatabaseObject {
  override def create: Update0 = null
  override def drop: Update0 = null
  lazy val manyToOne = ManyToOne(to, fromKey)
  lazy val oneToMany = OneToMany(to, fromKey)
  lazy val index = Index(name+"_ix", from, fromKey)
}

case class OptionalForeignKey[PK,FROM,TO](
  name: String,
  from: Table[_,FROM],
  to: Table[PK,TO],
  fromKey: FROM => PK
) extends DatabaseObject {
  override def create: Update0 = null
  override def drop: Update0 = null
  lazy val manyToOne = OptionalManyToOne(to, fromKey)
  lazy val oneToMany = OneToMany(to, fromKey)
  lazy val index = Index(name+"_ix", from, fromKey)
}


case class ManyToOne[FROM,PK,TO](
  to: Queryable[PK,TO],
  fromKey: FROM => PK
) {
  def join[K](q: Queryable[K,FROM]): Queryable[K, (FROM,TO)] =
    ManyToOneJoin(q, this)
  def ::[K](q: Queryable[K,FROM]) = join(q)

  def join[FROM2,PK2](m21: ManyToOne[FROM2,PK2,FROM]): ManyToOne[FROM2,PK2,(FROM,TO)] =
    ManyToOne(join(m21.to), m21.fromKey)
  def ::[FROM2,PK2](m21: ManyToOne[FROM2,PK2,FROM]): ManyToOne[FROM2,PK2,(FROM,TO)] =
    join(m21)

  def join[FROM2,PK2](m21: OptionalManyToOne[FROM2,PK2,FROM])
      :OptionalManyToOne[FROM2,PK2,(FROM,TO)] =  OptionalManyToOne(join(m21.to), m21.fromKey)
  def ::[FROM2,PK2](m21: OptionalManyToOne[FROM2,PK2,FROM])
      :OptionalManyToOne[FROM2,PK2,(FROM,TO)] =  join(m21)

  def join[FROM2,PK2](o2m:OneToMany[FROM2,PK2,FROM])
      :OneToMany[FROM2,PK2,(FROM,TO)] = OneToMany(join(o2m.to), o2m.fromKey)
  def ::[FROM2,PK2](o2m:OneToMany[FROM2,PK2,FROM])
      :OneToMany[FROM2,PK2,(FROM,TO)] = join(o2m)
}

case class ManyToOneJoin[K,FROM,PK,TO](
  left: Queryable[K,FROM],
  right: ManyToOne[FROM,PK,TO]
) extends Queryable[K,(FROM,TO)] {
  override def query(key: K): Stream[ConnectionIO,(FROM,TO)] = Stream()
}
 

case class OptionalManyToOne[FROM,PK,TO](
  to: Queryable[PK,TO],
  fromKey: FROM => PK
) {
  def join[K](q: Queryable[K,FROM]): Queryable[K, (FROM,Option[TO])]
     = OptionalManyToOneJoin(q, this)
  def ::[K](q: Queryable[K,FROM]) = join(q)

  def join[FROM2,PK2](m21: ManyToOne[FROM2,PK2,FROM]): ManyToOne[FROM2, PK2,(FROM,Option[TO])] =
    ManyToOne(join(m21.to), m21.fromKey)
  def ::[FROM2,PK2](m21: ManyToOne[FROM2,PK2,FROM]): ManyToOne[FROM2,PK2,(FROM,Option[TO])] =
    join(m21)

  def join[FROM2,PK2](m21: OptionalManyToOne[FROM2,PK2,FROM])
      :OptionalManyToOne[FROM2,PK2,(FROM,Option[TO])]
  = OptionalManyToOne(join(m21.to), m21.fromKey)
  def ::[FROM2,PK2](m21: OptionalManyToOne[FROM2,PK2,FROM])
      :OptionalManyToOne[FROM2,PK2,(FROM,Option[TO])] = join(m21)

  def join[FROM2,PK2](o2m:OneToMany[FROM2,PK2,FROM])
      :OneToMany[FROM2,PK2,(FROM,Option[TO])] = OneToMany(join(o2m.to), o2m.fromKey)
  def ::[FROM2,PK2](o2m:OneToMany[FROM2,PK2,FROM])
      :OneToMany[FROM2,PK2,(FROM,Option[TO])] = join(o2m)
}

case class OptionalManyToOneJoin[K,FROM,PK,TO](
  left: Queryable[K,FROM],
  right: OptionalManyToOne[FROM,PK,TO]
) extends Queryable[K,(FROM,Option[TO])] {
  override def query(key: K): Stream[ConnectionIO,(FROM,Option[TO])] = Stream()
}


case class OneToMany[FROM,PK,TO](
  to: Queryable[PK,TO],
  fromKey: FROM => PK
) {
  def join[K](q: Queryable[K,FROM]): Queryable[K, (FROM,Set[TO])] =
    OneToManyJoin(q,this)
  def ::[K](q: Queryable[K,FROM]) = join(q)

  def join[FROM2,PK2](m21: ManyToOne[FROM2,PK2,FROM]): ManyToOne[FROM2, PK2,(FROM,Set[TO])] =
    ManyToOne(join(m21.to), m21.fromKey)
  def ::[FROM2,PK2](m21: ManyToOne[FROM2,PK2,FROM]): ManyToOne[FROM2,PK2,(FROM,Set[TO])] =
    join(m21)

  def join[FROM2,PK2](m21: OptionalManyToOne[FROM2,PK2,FROM])
      :OptionalManyToOne[FROM2,PK2,(FROM,Set[TO])]
  = OptionalManyToOne(join(m21.to), m21.fromKey)
  def ::[FROM2,PK2](m21: OptionalManyToOne[FROM2,PK2,FROM])
      :OptionalManyToOne[FROM2,PK2,(FROM,Set[TO])] = join(m21)

  def join[FROM2,PK2](o2m:OneToMany[FROM2,PK2,FROM])
      :OneToMany[FROM2,PK2,(FROM,Set[TO])] = OneToMany(join(o2m.to), o2m.fromKey)
  def ::[FROM2,PK2](o2m:OneToMany[FROM2,PK2,FROM])
      :OneToMany[FROM2,PK2,(FROM,Set[TO])] = join(o2m)
}

case class OneToManyJoin[K,FROM,PK,TO](
  left: Queryable[K,FROM],
  right: OneToMany[FROM,PK,TO]
) extends Queryable[K, (FROM,Set[TO])] {
  override def query(key: K): Stream[ConnectionIO,(FROM,Set[TO])] = Stream()
}

