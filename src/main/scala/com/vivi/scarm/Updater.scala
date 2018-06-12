package com.vivi.scarm

import doobie._
import doobie.implicits._

import shapeless._
import shapeless.ops.hlist

/* 
 *  Used to provide two different methods of generating an update
 *  based on whether the key is a case class or just an atomic value.
 */

trait Updater[K,E] {
  def update(table: Table[K,E], entity: E): ConnectionIO[Int] 
}

trait LowPriorityUpdater {

  implicit def updaterForPrimitive[K,E,EList<:HList, REMList<:HList, REV,REVList<:HList](implicit
    eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    revList: hlist.Prepend.Aux[REMList,K::HNil,REVList],
    revComposite: Composite[REVList],
    revTupler: hlist.Tupler.Aux[REVList, REV]
  ) = new Updater[K,E] {
    def update(table: Table[K,E], entity: E) = {
      val key: K = table.primaryKey(entity)
      val remainder: REMList = remList(eGeneric.to(entity))
      val reversed: REVList = revList(remainder, key::HNil)
      Fragment(table.updateSql, reversed)(revComposite).update.run
    }
  }

}


object Updater extends LowPriorityUpdater {
  def apply[K,E](implicit updater: Updater[K,E]) = updater

  implicit def updaterForCaseClass[K,E,KList<:HList,EList<:HList, REMList<:HList, REV,REVList<:HList](implicit
    kGeneric: LabelledGeneric.Aux[K,KList],
    eGeneric: LabelledGeneric.Aux[E,EList],
    remList:  hlist.Drop.Aux[EList,Nat._1,REMList],
    revList: hlist.Prepend.Aux[REMList,KList,REVList],
    revComposite: Composite[REVList],
    revTupler: hlist.Tupler.Aux[REVList, REV]
  ) = new Updater[K,E] {
    def update(table: Table[K,E], entity: E) = {
      val key: KList = kGeneric.to(table.primaryKey(entity))
      val remainder: REMList = remList(eGeneric.to(entity))
      val reversed: REVList = revList(remainder, key)
      Fragment(table.updateSql, reversed)(revComposite).update.run
    }
  }
}

