/*
 * Copyright (C) 2017  Lightbend
 *
 * This file is part of flink-ModelServing
 *
 * flink-ModelServing is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.lightbend.modelServer.flinkprocessors

import com.lightbend.modelServer.model._
import com.lightbend.modelServer.model.speculative.{CurrentProcessing, ServingResponse, SpeculativeExecutionStats, SpeculativeServiceRequest}
import com.lightbend.modelServer.processor.VotingDecider
import com.lightbend.speculative.speculativedescriptor.SpeculativeDescriptor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object SpeculativeProcessor {
  val SERVERTIMEOUT = 150l
  def apply() = new SpeculativeProcessor
}

class SpeculativeProcessor extends CoProcessFunction[SpeculativeServiceRequest, SpeculativeDescriptor, ServingResult] {

  // The managed keyed state see https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/state.html
  var specualtiveState: ValueState[SpeculativeExecutionStats] = _
  val speculativeDecider = VotingDecider
  import SpeculativeProcessor._
  var speculativeTimeout: ValueState[Long] = _

  // Because we are explicitely using unique GUIDs, we do not need to bring this to the key scope
  // Using this in the keygroup scope is good enough.
  val currentProcessing = collection.mutable.Map[String, CurrentProcessing]()

  override def open(parameters: Configuration): Unit = {
    val speculativeModelDesc = new ValueStateDescriptor[SpeculativeExecutionStats](
      "currentSpeculativeModel",   // state name
      createTypeInformation[SpeculativeExecutionStats]) // type information
    speculativeModelDesc.setQueryable("speculativeState")
    specualtiveState = getRuntimeContext.getState(speculativeModelDesc)
    val speculativeTmoutDesc = new ValueStateDescriptor[Long] (
      "Timeout",
      createTypeInformation[Long])
    speculativeTimeout = getRuntimeContext.getState(speculativeTmoutDesc)
  }

  override def processElement2(model: SpeculativeDescriptor, ctx: CoProcessFunction[SpeculativeServiceRequest, SpeculativeDescriptor, ServingResult]#Context, out: Collector[ServingResult]): Unit = {

    speculativeTimeout.update((model.tmout))
    if(specualtiveState.value() == null) specualtiveState.update(SpeculativeExecutionStats(model.datatype, speculativeDecider.getClass.getName, model.tmout))
  }

  override def processElement1(record: SpeculativeServiceRequest, ctx: CoProcessFunction[SpeculativeServiceRequest, SpeculativeDescriptor, ServingResult]#Context, out: Collector[ServingResult]): Unit = {

    if(specualtiveState.value == null) specualtiveState.update(SpeculativeExecutionStats(record.dataType, speculativeDecider.getClass.getName, SERVERTIMEOUT))
    if(speculativeTimeout.value() == 0) speculativeTimeout.update(SERVERTIMEOUT)

    // This will be invoked in 3 different use cases - process accordingly
    record.result match {
      case Some(result) => // This is either intermediate result from Model Processor or result from Router (no models)
        record.models match {
          case m if (m == 0) => // from the router - just forward
            out.collect(result)
          case _ => // Intermediate result
            currentProcessing.get(record.GUID) match {
              case Some(processingResults) =>
                // We are still waiting for this GUID
                val current = CurrentProcessing(processingResults.dataType, processingResults.models, processingResults.start, processingResults.results += ServingResponse(record.GUID, result))
                current.results.size match {
                  case size if (size >= current.models) => processResult(record.GUID, current, out) // We are done
                  case _ => val _ = currentProcessing += (record.GUID -> current) // Keep going
                }
              case _ => // Timed out
            }
        }
      case _ => // This is start collection
        // Add to the watch list
        val currentTime = System.currentTimeMillis()
        currentProcessing += (record.GUID -> CurrentProcessing(record.dataType, record.models, currentTime, new ListBuffer[ServingResponse]()))
        // Schedule timeout
        ctx.timerService.registerProcessingTimeTimer(currentTime + speculativeTimeout.value())
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[SpeculativeServiceRequest, SpeculativeDescriptor, ServingResult]#OnTimerContext, out: Collector[ServingResult]): Unit = {
    // Complete timed out requests
    val start = System.currentTimeMillis() - speculativeTimeout.value
    currentProcessing.foreach(kv =>
      kv._2.start match {
        case t if (t < start) => // Timed out
          println(s"Timed out ${kv._1} at ${System.currentTimeMillis()} started at ${kv._2.start}")
          processResult(kv._1, kv._2, out)
        case _ => // keep going
      }
    )
  }

  // Complete speculative execution
  private def processResult(GUID : String, results: CurrentProcessing, out: Collector[ServingResult]) : Unit = {
    // Run it through decider
    val servingResult = speculativeDecider.decideResult(results).asInstanceOf[ServingResult]
    // Return valye
    out.collect(servingResult)
    // Update state
    if(servingResult.processed) {
      val state = specualtiveState.value().incrementUsage(servingResult.duration)
      specualtiveState.update(state)
    }
    // remove state
    val _ = currentProcessing -= GUID
  }
}