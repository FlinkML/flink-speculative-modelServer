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

package com.lightbend.modelServer.query

import com.lightbend.modelServer.model.ModelToServeStats
import com.lightbend.modelServer.model.speculative.SpeculativeExecutionStats
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
  * Created by boris on 5/12/17.
  * see https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/stream/queryable_state.html
  */
object ModelStateQuery {

  val timeInterval = 1000 * 20        // 20 sec

  def main(args: Array[String]) {

    val jobId = JobID.fromHexString("ee5332de3f8e0fe9320b69d08b1a1b78")
    val types = Array("cpu")

    val client = new QueryableStateClient("127.0.0.1", 9069)

    // State descriptor for individual mpdels
    val currentModelsDesc = new ValueStateDescriptor[ListBuffer[String]](
      "modelList", // state name
      createTypeInformation[ListBuffer[String]].createSerializer(new ExecutionConfig)  // type Serializer
    )

    // State descriptor for voting model.
    val speculativeModelDesc = new ValueStateDescriptor[SpeculativeExecutionStats](
      "currentSpeculativeModel",   // state name
      createTypeInformation[SpeculativeExecutionStats].createSerializer(new ExecutionConfig)  // type Serializer
    )

    // State descriptor for state of individual model.
    val modelStateDesc = new ValueStateDescriptor[ModelToServeStats](
      "currentModelState",                  // state name
      createTypeInformation[ModelToServeStats].createSerializer(new ExecutionConfig)  // type Serializer
    )

    val keyType = BasicTypeInfo.STRING_TYPE_INFO

    while(true) {
      for (key <- types) {
        try {
          // Get model list
          val modelsListFuture = client.getKvState(jobId, "currentModels", key, keyType, currentModelsDesc)
          val models = modelsListFuture.join().value()
          println(s"Currrent models for type $key is $models")
          // Individual models
          models.foreach(model => {
            val modelStateFuture = client.getKvState(jobId, "currentModelState", model, keyType, modelStateDesc)
            val modelState = modelStateFuture.join().value()
            println(s"Model ${modelState.description}, deployed at ${new DateTime(modelState.since).toString("yyyy/MM/dd HH:MM:SS")}}. " +
              s"Average execution ${modelState.duration/modelState.usage}, min execution ${modelState.min}, max execution ${modelState.max}")
          })
          // Speculative model
          val speculativeStateFuture = client.getKvState(jobId, "speculativeState", key, keyType, speculativeModelDesc)
          val speculativeState = speculativeStateFuture.join().value()
          println(s"Speculative Model for type $key is" +
            s"Deployed at ${new DateTime(speculativeState.since).toString("yyyy/MM/dd HH:MM:SS")}} with decider ${speculativeState.decider} " +
            s"and timeout ${speculativeState.tmout}. Average execution ${speculativeState.duration/speculativeState.usage}, min execution ${speculativeState.min}, max execution ${speculativeState.max}")
        }
        catch {case e: Exception => e.printStackTrace()}
      }
      Thread.sleep(timeInterval)
    }
  }
}