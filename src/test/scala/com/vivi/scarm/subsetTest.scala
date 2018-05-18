package com.vivi.scarm

import shapeless._

case class AClass(x: Int, y: String)

object SubsetTest {
  val w = Subset[HNil, Int :: HNil]
  val x = Subset[Int, Int :: HNil]
  val y = Subset[Int, String :: Int :: HNil]
  val z = Subset[String :: Int :: HNil, String :: Int :: HNil]
}
