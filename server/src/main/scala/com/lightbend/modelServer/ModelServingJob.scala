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

package com.lightbend.modelServer


import java.util._

import com.lightbend.kafka.configuration.ModelServingConfiguration
import com.lightbend.modelServer.handlers.BadDataHandler
import com.lightbend.modelServer.model.speculative.{SpeculativeConverter, SpeculativeModel, SpeculativeRequest}
import com.lightbend.modelServer.flinkprocessors.{ModelProcessor, RouterProcessor, SpeculativeProcessor}
import com.lightbend.modelServer.model.{DataRecord, ModelToServe}
import com.lightbend.modelServer.typeschema.ByteArraySchema
import com.lightbend.model.cpudata.CPUData
import com.lightbend.speculative.speculativedescriptor.SpeculativeDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.configuration._
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
  * Created by boris on 5/9/17.
  * loosely based on http://dataartisans.github.io/flink-training/exercises/eventTimeJoin.html approach
  * for queriable state
  *   https://github.com/dataArtisans/flink-queryable_state_demo/blob/master/README.md
  * Using Flink min server to enable Queryable data access
  *   see https://github.com/dataArtisans/flink-queryable_state_demo/blob/master/src/main/java/com/dataartisans/queryablestatedemo/EventCountJob.java
  *
  * This little application is based on a RichCoProcessFunction which works on a keyed streams. It is applicable
  * when a single applications serves multiple different models for different data types. Every model is keyed with
  * the type of data what it is designed for. Same key should be present in the data, if it wants to use a specific
  * model.
  * Scaling of the application is based on the data type - for every key there is a separate instance of the
  * RichCoProcessFunction dedicated to this type. All messages of the same type are processed by the same instance
  * of RichCoProcessFunction
  */
object ModelServingJob {

  def main(args: Array[String]): Unit = {
//    executeLocal()
    executeServer()
  }

  // Execute on the local Flink server - to test queariable state
  def executeServer() : Unit = {

    // We use a mini cluster here for sake of simplicity, because I don't want
    // to require a Flink installation to run this demo. Everything should be
    // contained in this JAR.

    val port = 6124
    val parallelism = 2

    val config = new Configuration()
    config.setInteger(JobManagerOptions.PORT, port)
    config.setString(JobManagerOptions.ADDRESS, "localhost")

    // In a non MiniCluster setup queryable state is enabled by default.
    config.setString(QueryableStateOptions.PROXY_PORT_RANGE, "9069")
    config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 2)
    config.setInteger(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS, 2)

    config.setString(QueryableStateOptions.SERVER_PORT_RANGE, "9067")
    config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 2)
    config.setInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS, 2)


    // Create a local Flink server
    val flinkCluster = new LocalFlinkMiniCluster(
      config,
      HighAvailabilityServicesUtils.createHighAvailabilityServices(
        config,
        Executors.directExecutor(),
        HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION),
      false)
    try {
      // Start server and create environment
      flinkCluster.start(true)
      val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkCluster.getLeaderRPCPort)
       // Build Graph
      buildGraph(env)
      val jobGraph = env.getStreamGraph.getJobGraph
      // Submit to the server and wait for completion
      flinkCluster.submitJobAndWait(jobGraph, false)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // Execute localle in the environment
  def executeLocal() : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    buildGraph(env)
    System.out.println("[info] Job ID: " + env.getStreamGraph.getJobGraph.getJobID)
    env.execute()
  }

  // Build execution Graph
  def buildGraph(env : StreamExecutionEnvironment) : Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    // Side outputs
    val modelTag = OutputTag[SpeculativeModel]("speculative-model")
    val dataTag = OutputTag[SpeculativeRequest]("speculative-data")


    import ModelServingConfiguration._

    // configure Kafka consumer

    // Data
    val dataKafkaProps = new Properties
    dataKafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    dataKafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    dataKafkaProps.setProperty("group.id", DATA_GROUP)
    // always read the Kafka topic from the current location
    dataKafkaProps.setProperty("auto.offset.reset", "latest")

    // Model
    val modelKafkaProps = new Properties
    modelKafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    modelKafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    modelKafkaProps.setProperty("group.id", MODELS_GROUP)
    // always read the Kafka topic from the current location
    modelKafkaProps.setProperty("auto.offset.reset", "earliest")

    // Speculative configuration
    val speculativeKafkaProps = new Properties
    speculativeKafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    speculativeKafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    speculativeKafkaProps.setProperty("group.id", SPECULATIVE_GROUP)
    // always read the Kafka topic from the current location
    speculativeKafkaProps.setProperty("auto.offset.reset", "earliest")

    // create a Kafka consumers

    // Data
    val dataConsumer = new FlinkKafkaConsumer011[Array[Byte]](
      DATA_TOPIC,
      new ByteArraySchema,
      dataKafkaProps
    )

    // Model
    val modelConsumer = new FlinkKafkaConsumer011[Array[Byte]](
      MODELS_TOPIC,
      new ByteArraySchema,
      modelKafkaProps
    )

    // Speculative
    val speculativeConsumer = new FlinkKafkaConsumer011[Array[Byte]](
      SPECULATIVE_TOPIC,
      new ByteArraySchema,
      speculativeKafkaProps
    )

    // Create input data streams
    val dataStream = env.addSource(dataConsumer)
    val modelsStream = env.addSource(modelConsumer)
    val speculativeStream = env.addSource(speculativeConsumer)

    // Read data from streams
    val models = modelsStream.map(ModelToServe.fromByteArray(_))
      .flatMap(BadDataHandler[ModelToServe])
      .keyBy(_.dataType)
    val data = dataStream.map(DataRecord.fromByteArray(_))
      .flatMap(BadDataHandler[CPUData])
      .keyBy(_.dataType)
    val config = speculativeStream.map(SpeculativeConverter.fromByteArray(_))
      .flatMap(BadDataHandler[SpeculativeDescriptor])
      .keyBy(_.datatype)

    // Route models and data
    val speculativeStart = data
      .connect(models)
      .process(RouterProcessor())

    // Pickup side inputs and process them
    val modelUpdate = speculativeStart.getSideOutput(modelTag).keyBy(_.dataModel)
    val dataProcess = speculativeStart.getSideOutput(dataTag).keyBy(_.dataModel)

    // Process individual model
    val individualresult = dataProcess
      .connect(modelUpdate)
      .process(ModelProcessor())

    // Run voting
    speculativeStart.union(individualresult).keyBy(_.dataType)
      .connect(config)
      .process(SpeculativeProcessor())
      .map(result => {
        result.processed match {
          case true =>
            result.result match{
              case Some(r) => println(s"Using model ${result.model}. Calculated result $r, expected ${result.source} calculated in ${result.duration} ms")
              case _ =>
            }
          case _ => println("No model available - skipping")
        }
      })
  }
}