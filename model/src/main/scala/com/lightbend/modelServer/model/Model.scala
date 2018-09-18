package com.lightbend.modelServer.model


/**
 * Created by boris on 5/9/17.
 * Basic trait for models. For simplicity, we assume the data to be scored are WineRecords.
 */
trait Model {
  def score(record: AnyVal): Option[AnyVal]
  def cleanup(): Unit
  def toBytes(): Array[Byte]
  def getType : Long
}
