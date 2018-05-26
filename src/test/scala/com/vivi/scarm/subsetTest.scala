package com.vivi.scarm.subsetTest

import com.vivi.scarm.{Subset,StructurallyEqual,HeadIsStructurallyEqual}

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


case class WrappedProjection(id: Projection1)

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

class StructuralEqualityTest extends FunSuite {
  val a = StructurallyEqual[Int,Int]
  val b = StructurallyEqual[(Int,String),(Int,String)]
  val c = StructurallyEqual[Projection1,Projection1]
  val d = StructurallyEqual[Projection1,Projection2]
  val hd = HeadIsStructurallyEqual[WrappedProjection,Projection1]

  test("not everything is equal") {
    assertDoesNotCompile(
      "val e = StructurallyEqual[Int,String]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[(Int,Int),(Int,String)]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[(String,Int),(Int,String)]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[(String,Int),(String)]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[(String),(String,Int)]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[Projection1,Projection3]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[Projection1,Projection4]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[Projection1,Projection5]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[Projection1,Projection6]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[Projection1,Projection7]"
    )
    assertDoesNotCompile(
      "val e = StructurallyEqual[Projection7,Projection1)"
    )
  }
}


case class Inner(x: Int, y: Int)
case class Outer(x: Int, nm: String, inner: Inner)

case class InnerSub(x: Int)
case class OuterSub(x: Int, inner: InnerSub, nm:String)

class NestedSubsetTest extends FunSuite {
  val a = Subset[OuterSub,Outer]
}
