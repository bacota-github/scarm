package com.vivi.scarm.subsetTest

import com.vivi.scarm.{Catenation,Flattened,Subset,StructurallyEqual,SimilarStructure}

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

case class Projection8(x: Option[Int], y: Int)
case class Projection9(x:Int, y:Int, z: Int)

case class WrappedProjection(id: Projection1)
case class WrappedProjection2(z: String, id: Projection1, x: Int)
case class DeeplyWrappedProjection(wrapped: WrappedProjection)
case class DeeplyDeeplyWrappedProjection(wrapped: DeeplyWrappedProjection)

case class WrappedOption(id: Option[Projection1])


class ShapelessTest extends FunSuite {

  test("Subsets") {
    val a = Subset[Projection1,Projection1]
    val b = Subset[Projection1,Projection2]
    val c = Subset[Projection1,Projection3]
    val d = Subset[Projection1,Projection4]
    val e = Subset[WrappedProjection, WrappedProjection2]
  }
/*
  test("Structural Similarity") {
    val a = SimilarStructure[Projection1, Projection1]
    val b = SimilarStructure[Projection1, Projection2]
    val c = SimilarStructure[Projection1, Int::Int::HNil]
    val d = SimilarStructure[WrappedProjection, Projection1]
    val e = SimilarStructure[WrappedProjection, Projection2]
    val f = SimilarStructure[DeeplyWrappedProjection, Projection2]
    val g = SimilarStructure[Int::Int::HNil, Projection1]
    val h = SimilarStructure[Int::Int::HNil, Int::Int::HNil]
    val i = SimilarStructure[Int::HNil, Int::Int::HNil]
    val j = SimilarStructure[Int::String::HNil, Int::Int::HNil]
    val k = SimilarStructure[Int::Int::String::HNil, Int::HNil]
    val l = SimilarStructure[Int::String::HNil, Int::Int::HNil]
    val m = SimilarStructure[WrappedProjection2, WrappedProjection]
  }
 */

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
