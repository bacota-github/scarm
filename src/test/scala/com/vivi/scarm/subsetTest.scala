package com.vivi.scarm.test

import com.vivi.scarm.Subset

import org.scalatest._
import shapeless._

class SubsetTest extends FunSuite {
  val w = Subset[HNil, Int :: HNil]
  val x = Subset[Int, Int :: HNil]
  val y = Subset[Int, String :: Int :: HNil]
  val z = Subset[String :: Int :: HNil, String :: Int :: HNil]

  test("not everything is a subset") {

    assertDoesNotCompile(
      "val mismatch = Subset[String, Int :: HNil]"
    )

    assertDoesNotCompile(
      "val mismatch = Subset[String :: Int, Int :: HNil]"
    )

    assertDoesNotCompile(
      "val mismatch = Subset[Int :: String, Float :: Int :: HNil]"
    )

    assertDoesNotCompile(
      "val mismatch = Subset[Int, HNil]"
    )
  }
}
