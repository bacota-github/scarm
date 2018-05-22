package com.vivi.scarm.test

import com.vivi.scarm.PrimaryKey 

import org.scalatest._
import shapeless._

case class PrimaryKeyClass(x: Int, y: String)
case class PrimaryKeyNestedClass(pk: PrimaryKeyClass, x: Int, y: Int)


class PrimaryKeyTest extends FunSuite {

  test("primary key of simple hlist") {
    val pk = PrimaryKey[Int, Int :: HNil]
    assert(pk(1 :: HNil) == 1)
  }

  test("primary key of longer hlist") {
    val pk = PrimaryKey[Int, Int :: String :: HNil]
    assert(pk(1 :: "2" :: HNil) == 1)
  }

  test ("primary key can be a case class") {
    val pk = PrimaryKey[PrimaryKeyClass, PrimaryKeyClass :: Int :: HNil]
    val x = PrimaryKeyClass(1,"2")
    assert(pk(x :: 99 :: HNil) == x)
  }

  test("primary key of a case class") {
    val pk = PrimaryKey[Int, PrimaryKeyClass]
    val x = PrimaryKeyClass(10, "10")
    assert(pk(x) == 10)
  }

  test("Primary key of a case class that is, itself a case class") {
    val pk = PrimaryKey[PrimaryKeyClass, PrimaryKeyNestedClass]
    val x = PrimaryKeyNestedClass(PrimaryKeyClass(20,"a"), 11,12)
    assert(pk(x) == x.pk)
  }

  test("Not everything is a primary key") {
    assertDoesNotCompile("val pk = PrimaryKey[Int, HNil]")
    assertDoesNotCompile("val pk = PrimaryKey[Int, String :: HNil]")
    assertDoesNotCompile("val pk = PrimaryKey[String, PrimaryKeyClass]")
  }
}
