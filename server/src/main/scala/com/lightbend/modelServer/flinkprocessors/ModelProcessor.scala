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
import com.lightbend.modelServer.model.speculative.{SpeculativeModel, SpeculativeRequest, SpeculativeServiceRequest}
import com.lightbend.modelServer.typeschema.ModelTypeSerializer
import com.lightbend.model.cpudata.CPUData
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
  * Created by boris on 5/8/17.
  *
  * Main class processing data using models
  *
  * see http://dataartisans.github.io/flink-training/exercises/eventTimeJoin.html for details
  */

object ModelProcessor {
  def apply() = new ModelProcessor
}

class ModelProcessor extends CoProcessFunction[SpeculativeRequest, SpeculativeModel, SpeculativeServiceRequest] {

  // The managed keyed state see https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/state.html
  // In Flink class instance is created not for key, but rater key groups
  // https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/state/state.html#keyed-state-and-operator-state
  // As a result, any key specific sate data has to be in the key specific state

  var modelState: ValueState[ModelToServeStats] = _
  var newModelState: ValueState[ModelToServeStats] = _

  var currentModel : ValueState[Option[Model]] = _
  var newModel : ValueState[Option[Model]] = _

  override def open(parameters: Configuration): Unit = {

    val modelStateDesc = new ValueStateDescriptor[ModelToServeStats](
      "currentModelState",                  // state name
      createTypeInformation[ModelToServeStats])     // type information
    modelStateDesc.setQueryable("currentModelState")     // Expose it for queryable state
    modelState = getRuntimeContext.getState(modelStateDesc)
    val newModelStateDesc = new ValueStateDescriptor[ModelToServeStats](
      "newModelState",                      // state name
      createTypeInformation[ModelToServeStats])     // type information
    newModelState = getRuntimeContext.getState(newModelStateDesc)
    val modelDesc = new ValueStateDescriptor[Option[Model]](
      "currentModel",                               // state name
      new ModelTypeSerializer)                      // type information
    currentModel = getRuntimeContext.getState(modelDesc)
    val newModelDesc = new ValueStateDescriptor[Option[Model]](
      "newModel",                                   // state name
      new ModelTypeSerializer)                       // type information
    newModel = getRuntimeContext.getState(newModelDesc)
  }

  override def processElement2(model: SpeculativeModel, ctx: CoProcessFunction[SpeculativeRequest, SpeculativeModel, SpeculativeServiceRequest]#Context, out: Collector[SpeculativeServiceRequest]): Unit = {

    if(newModel.value == null) newModel.update(None)
    if(currentModel.value == null) currentModel.update(None)

    println(s"Model Processor new model - ${model.model}")
    ModelToServe.toModel(model.model) match {
      case Some(md) =>
        newModelState.update (ModelToServeStats(model.model))
        newModel.update(Some(md))
      case  _ =>
    }
  }

  override def processElement1(record: SpeculativeRequest, ctx: CoProcessFunction[SpeculativeRequest, SpeculativeModel, SpeculativeServiceRequest]#Context, out: Collector[SpeculativeServiceRequest]): Unit = {

    // See if we have update for the model
    newModel.value.foreach { model =>
      // close current model first
      currentModel.value.foreach(_.cleanup())
      // Update model
      currentModel.update(newModel.value)
      modelState.update(newModelState.value)
      newModel.update(None)
    }

    // Actually process data
    currentModel.value match {
      case Some(model) => {
        val start = System.currentTimeMillis()
        val result = model.score(record.data)
        val duration = System.currentTimeMillis() - start
        modelState.value match{
          case currentstats if(currentstats != null) =>
            modelState.update(currentstats.incrementUsage(duration))
          case currentstats => println(s"Oops - current state (${record.dataModel})== $currentstats")
        }
//        println(s"Calculated result - $result calculated in $duration ms")
        val cpudata = record.data.asInstanceOf[CPUData]
        out.collect(SpeculativeServiceRequest(cpudata.dataType, record.dataType, record.GUID,
          Some(ServingResult(modelState.value().description, cpudata._class, result.asInstanceOf[Option[Int]], duration)), 1))
      }
      case _ => // println("No model available - skipping")
    }
  }
}
