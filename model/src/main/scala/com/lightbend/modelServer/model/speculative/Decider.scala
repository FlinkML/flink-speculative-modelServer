package com.lightbend.modelServer.model.speculative

import scala.collection.mutable.ListBuffer
import com.lightbend.modelServer.model.ServingResult

trait Decider {

  def decideResult(results: CurrentProcessing): ServingResult
}

case class CurrentProcessing(dataType : String, models : Int, start : Long, results : ListBuffer[ServingResponse])
