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

import java.util.UUID

import com.lightbend.modelServer.model._
import com.lightbend.modelServer.model.speculative.{SpeculativeModel, SpeculativeRequest, SpeculativeServiceRequest}
import com.lightbend.model.cpudata.CPUData
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object RouterProcessor {
  def apply() = new RouterProcessor
}

class RouterProcessor extends CoProcessFunction[CPUData, ModelToServe, SpeculativeServiceRequest] {

  // Side output tags
  val modelTag = OutputTag[SpeculativeModel]("speculative-model")
  val dataTag = OutputTag[SpeculativeRequest]("speculative-data")

  // In Flink class instance is created not for key, but rater key groups
  // https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/state/state.html#keyed-state-and-operator-state
  // As a result, any key specific sate data has to be in the key specific state
  // Known models
  var currentModels : ValueState[ListBuffer[String]] = _

  override def open(parameters: Configuration): Unit = {

    val currentModelsDesc = new ValueStateDescriptor[ListBuffer[String]](
      "modelList", // state name
      createTypeInformation[ListBuffer[String]]) // type information
    currentModelsDesc.setQueryable("currentModels") // Expose it for queryable state
    currentModels = getRuntimeContext.getState(currentModelsDesc)
  }

  override def processElement2(model: ModelToServe, ctx: CoProcessFunction[CPUData, ModelToServe, SpeculativeServiceRequest]#Context, out: Collector[SpeculativeServiceRequest]): Unit = {

    if(currentModels.value == null) currentModels.update(new ListBuffer[String])

    println(s"Router Processor new model - $model")

    // Update known models if necessary
    val dataModel = s"${model.dataType}-${model.name}"
    val currentModel = currentModels.value
    if(!currentModel.exists(e => dataModel == e)) {
      currentModel.append(dataModel)
      currentModels.update(currentModel)
    }
    println(s"current models - $currentModels")

    // emit data to side output
    println(s"emmiting model with the key - $dataModel")
    ctx.output(modelTag, SpeculativeModel(dataModel, model))
  }

  override def processElement1(record: CPUData, ctx: CoProcessFunction[CPUData, ModelToServe, SpeculativeServiceRequest]#Context, out: Collector[SpeculativeServiceRequest]): Unit = {

    if(currentModels.value == null) currentModels.update(new ListBuffer[String])
    val GUID = UUID.randomUUID().toString
    // See if we have any models for this
    val currentModel = currentModels.value()
    currentModel.size match {
      case length if(length > 0) =>
        out.collect(SpeculativeServiceRequest(record.dataType, "", GUID, None, length))
        currentModel.foreach(dm =>
          ctx.output(dataTag, SpeculativeRequest(dm, record.dataType, GUID, record.asInstanceOf[AnyVal]))
        )
      case _ => out.collect(SpeculativeServiceRequest(record.dataType, "", GUID, Some(ServingResult.noModel)))
    }
  }
}