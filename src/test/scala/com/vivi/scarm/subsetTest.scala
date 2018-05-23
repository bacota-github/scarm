package com.vivi.scarm.subsetTest

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

case class Projection1(x: Int, y: Int)
case class Projection2(x: Int, y: Int)
case class Projection3(y: Int, x: Int)
case class Projection4(y: Int, z: Int, x: Int)
case class Projection5(x: Int, z: Int)
case class Projection6(x: Int, y: String)
case class Projection7(y: Int, z: Int, x: String)


class ProjectionTest extends FunSuite {

  val a = Subset[Projection1,Projection1]
  val b = Subset[Projection1,Projection2]
  val c = Subset[Projection1,Projection3]
  val d = Subset[Projection1,Projection4]

  test("not everything is a projection") {
    assertDoesNotCompile(
      "val e = Subset[Projection1,Projection5]"
    )
    assertDoesNotCompile(
      "val e = Subset[Projection1,Projection6]"
    )
    assertDoesNotCompile(
      "val e = Subset[Projection1,Projection7]"
    )
  }
}
