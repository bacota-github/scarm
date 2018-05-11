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
}
