package com.vivi.scarm

import doobie.imports._
import cats.effect.IO
import doobie.util.transactor.Transactor
import java.util.UUID
   
object Demo {
  def main: Unit = {
    val xa = Transactor.fromDriverManager[IO](
        "org.h2.Driver", "jdbc:h2:mem:db" , "" , ""
    )
    def run[A](query: ConnectionIO[A]): A =
      query.transact(xa).unsafeRunSync

    val prepareDb: ConnectionIO[Int] =
      sql"""create table if not exists people
          ( id char(36) not null unique
          , name varchar(512) not null
          , email varchar(512) not null unique
          , primary key(id)
          )
     """.update.run

    case class NewPerson(name: String, email: String)

    implicit val UUIDMeta: Meta[UUID] =
      Meta[String].xmap(UUID.fromString, _.toString)

    def addPerson(x: NewPerson): ConnectionIO[Int] =
      sql"""insert into people (id, name, email)
        values ( ${UUID.randomUUID}
               , ${x.name}
               , ${x.email}
               )
     """.update.run

    case class Person(id: UUID, name: String, email: String)

    def getPeople: ConnectionIO[List[Person]] =
      (sql"""select id, name, email
         from people
      """.query[Person]
      ).list

    val composed: ConnectionIO[List[Person]] = for {
      _ <- prepareDb
      _ <- addPerson(NewPerson("James Earl Douglas", "james@earldouglas.com"))
      _ <- addPerson(NewPerson("Johnny McDoe", "johnny@mcdoe"))
      l <- getPeople
    } yield l

    val l = run(composed)

    l.foreach {
          case Person(id, name, email) =>
            println()
            println(s"## ${name}")
            println()
            println(s"* ID: ${id.toString}")
            println(s"* Email: ${email}")
    }
  }
}
