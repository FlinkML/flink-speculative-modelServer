package com.lightbend.modelServer.processor

import com.lightbend.modelServer.model.ServingResult
import com.lightbend.modelServer.model.speculative.{CurrentProcessing, Decider}


object VotingDecider extends Decider with Serializable{

  // The simple voting decider for results 0 or 1. Returning 0 or 1
  override def decideResult(results: CurrentProcessing): ServingResult = {

    var result = ServingResult.noModel
    var sum = .0
    var count = 0
    var source = 0
    results.results.foreach(res => res.result match {
      case r if(r.processed) =>
        sum = sum + r.result.getOrElse(0).asInstanceOf[Int]
        if(r.source != source) source = r.source
        count = count + 1
      case _ =>
    })
    if(count == 0) result
    else {
      val res = if(sum/count < .5) 0 else 1
      ServingResult("voter model", source, Some(res), System.currentTimeMillis() - results.start)
    }
  }
}
