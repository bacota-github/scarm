package com.vivi

import doobie._
import doobie.implicits._


package object scarm {
  implicit lazy val JavaTimeLocalTimeMeta: Meta[java.time.LocalTime] =
    Meta[java.sql.Time].xmap(_.toLocalTime, java.sql.Time.valueOf)
}
