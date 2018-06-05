package com.vivi.scarm.test

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import java.time._

import org.scalatest._

import com.vivi.scarm._

import com.vivi.scarm.JavaTimeLocalDateMeta

case class DateEntity(id: Id,
  instant: Instant = Instant.now,
  localDate: LocalDate = LocalDate.now,
  localDateTime: LocalDateTime = LocalDateTime.now,
  localTimex: LocalTime = LocalTime.now
)


case class TestJavaTime(
  override val xa: Transactor[IO],
  override val dialect: SqlDialect,
  override val cleanup: (Transactor[IO] => Boolean) = (_ => true )
) extends FunSuite with DSLTestBase  {
  val dateTable = Table[Id,DateEntity]("date")
  override val  allTables = Seq(dateTable)

  test("java.time fields are supported") {
    val newDate = DateEntity(nextId)
    val table = dateTable
    run(table.insert(newDate))
    val readDate = run(table(newDate.id))
    assert(!readDate.isEmpty)
    val read = readDate.get

    val originalEpoch = newDate.instant.getEpochSecond()
    val readEpoch = read.instant.getEpochSecond()
    assert(Math.abs(readEpoch - originalEpoch) <= 1)
    assert(read.localDate == newDate.localDate)

    val rdate = read.localDateTime
    val ndate = newDate.localDateTime
    assert(rdate.getYear() == ndate.getYear())
    assert(rdate.getMonth() == ndate.getMonth())
    assert(rdate.getDayOfMonth() == ndate.getDayOfMonth())
    assert(rdate.getDayOfWeek() == ndate.getDayOfWeek())
    assert(rdate.getDayOfYear() == ndate.getDayOfYear())
    assert(rdate.getHour() == ndate.getHour())
    assert(Math.abs(rdate.getMinute() -ndate.getMinute()) <= 1)
    assert(Math.abs(rdate.getSecond() - ndate.getSecond()) <= 1)

    val rtime = read.localTimex
    val ntime = newDate.localTimex
    assert(rtime.getHour() == ntime.getHour())
    assert(rtime.getMinute() == ntime.getMinute())
    assert(rtime.getSecond() == ntime.getSecond())
  }
}
