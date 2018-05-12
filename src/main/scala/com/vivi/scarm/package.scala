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

  implicit def JavaTimeLocalDateMeta(implicit dialect: SQLDialect): Meta[java.time.LocalDate] =
    Meta[java.sql.Date].xmap(
      d => { if (dialect == Mysql) d.toLocalDate.plusDays(1) else d.toLocalDate },
      java.sql.Date.valueOf
    )

  trait SQLDialect
  object Postgresql extends SQLDialect
  object Mysql extends SQLDialect
  object Hsqldb extends SQLDialect
}
