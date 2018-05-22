package com.vivi.scarm

import shapeless._
import shapeless.ops.hlist.IsHCons

trait PrimaryKey[K,E]  {
  def apply(entity: E): K
}


trait LowerPriorityPrimaryKey {
  implicit def primaryKeyOfNonHList[K,E,Repr<:HList]
    (implicit generic: Generic.Aux[E,Repr],
      isHlistPrimaryKey: PrimaryKey[K,Repr],
      hlistPrimaryKey: PrimaryKey[K,Repr]
    ) = new PrimaryKey[K, E] {
    def apply(entity: E): K = hlistPrimaryKey(generic.to(entity))
  }
}


object PrimaryKey extends LowerPriorityPrimaryKey{ 
  def apply[K,E](implicit pk: PrimaryKey[K,E]) = pk

  implicit def primaryKeyOfHList[HD, TL<:HList] =  new PrimaryKey[HD, HD::TL] {
    def apply(entity: HD::TL) = entity.head
  }
}
