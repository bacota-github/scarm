package com.vivi.scarm.subsetTest

import com.vivi.scarm.{Catenation,Flattened,Subset,StructurallyEqual}

import org.scalatest._
import shapeless._


case class Struct(x: Int, name: String)
case class Struct1(s: Struct, name: String)
case class Struct2(z: Int, s: Struct)

case class Wrapper(x: Int)
case class Wrapper2(w: Wrapper)

case class Projection1(x: Int, y: Int)
case class Projection2(x: Int, y: Int)
case class Projection3(y: Int, x: Int)
case class Projection4(y: Int, z: Int, x: Int)
case class Projection5(x: Int, z: Int)
case class Projection6(x: Int, y: String)
case class Projection7(y: Int, z: Int, x: String)

case class WrappedProjection(id: Projection1)
case class WrappedProjection2(z: String, id: Projection1, x: Int)
case class DeeplyWrappedProjection(wrapped: WrappedProjection)
case class DeeplyDeeplyWrappedProjection(wrapped: DeeplyWrappedProjection)


class ShapelessTest extends FunSuite {

  test("Subsets") {
    val a = Subset[Projection1,Projection1]
    val b = Subset[Projection1,Projection2]
    val c = Subset[Projection1,Projection3]
    val d = Subset[Projection1,Projection4]
    val e = Subset[WrappedProjection, WrappedProjection2]
  }

  test("Structural Equality") {
    val a = StructurallyEqual[Projection1, Projection1]
    implicit val flat1 = Flattened[Projection1, Int::Int::HNil]
//    implicit val flat2 = Flattened[Projection2, Int::Int::HNil]
    val b = StructurallyEqual[Projection1, Projection2]
    val c = StructurallyEqual[Projection1, Int::Int::HNil]
    implicit val flat3 = Flattened[WrappedProjection, Int::Int::HNil]
    val d = StructurallyEqual[WrappedProjection, Projection1]
    val e = StructurallyEqual[WrappedProjection, Projection2]
    implicit val flat4 = Flattened[DeeplyWrappedProjection, Int::Int::HNil]
    val f = StructurallyEqual[DeeplyWrappedProjection, Projection2]
    val g = StructurallyEqual[Int::Int::HNil, Projection1]
    val h = StructurallyEqual[Int::Int::HNil, Int::Int::HNil]
  }

  test("Flattened") {
    val a = Flattened[Projection1, Projection1]
    val b = Flattened[Projection1, Int::Int::HNil]
    val c = Flattened[Int::Int::HNil, Int::Int::HNil]
    val d = Flattened[WrappedProjection, Int::Int::HNil]
    val e = Flattened[DeeplyWrappedProjection, WrappedProjection::HNil]
    implicit val f = Flattened[WrappedProjection, Int::Int::HNil]
    val g = Flattened[DeeplyWrappedProjection, Int::Int::HNil]
    val h = Flattened[Struct, Int::String::HNil]
    val i = Flattened[Struct1, Struct::String::HNil]
    val j = Flattened[Struct1, Int::String::String::HNil]
    val k = Flattened[Struct2, Int::Struct::HNil]
    val l = Flattened[Struct2, Int::Int::String::HNil]
  }
}
