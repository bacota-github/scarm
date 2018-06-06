package com.vivi

import doobie._
import doobie.implicits._


package object scarm {

  implicit lazy val JavaTimeLocalTimeMeta: Meta[java.time.LocalTime] =
    Meta[java.sql.Time].xmap(_.toLocalTime, java.sql.Time.valueOf)

  implicit lazy val JavaDate: Meta[java.util.Date] =
    Meta[java.sql.Date].xmap(d => d, d => new java.sql.Date(d.getTime()))

  implicit lazy val JavaTimeLocalDateTimeMeta: Meta[java.time.LocalDateTime] =
    Meta[java.sql.Timestamp].xmap(_.toLocalDateTime, java.sql.Timestamp.valueOf)

  implicit def JavaTimeLocalDateMeta(implicit dialect: SqlDialect): Meta[java.time.LocalDate] =
    Meta[java.sql.Date].xmap(
      d => { if (dialect == Mysql) d.toLocalDate.plusDays(1) else d.toLocalDate },
      java.sql.Date.valueOf
    )

  import java.util.UUID
  implicit lazy val UUIDMeta: Meta[UUID] = Meta[String].xmap(UUID.fromString(_), _.toString)

  trait SqlDialect
  object Postgresql extends SqlDialect
  object Mysql extends SqlDialect
  object Hsqldb extends SqlDialect
}
