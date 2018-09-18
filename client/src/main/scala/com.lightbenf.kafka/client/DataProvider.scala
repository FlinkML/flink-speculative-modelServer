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

package com.lightbend.kafka.client

import java.io._

import com.google.protobuf.ByteString
import com.lightbend.kafka.{KafkaLocalServer, MessageSender}

import scala.concurrent.Future
import com.lightbend.kafka.configuration.ModelServingConfiguration._
import com.lightbend.model.cpudata.CPUData
import com.lightbend.model.modeldescriptor.{ModelDescriptor, ModelPreprocessing}
import com.lightbenf.kafka.generator.{MarkovChain, Model, Noise, RandomPulses}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

/**
 * Created by boris on 5/10/17.
 *
 * Application publishing models from /data directory to Kafka
 */
object DataProvider {

  val dataTimeInterval = 1000             // 1 sec
  val directory = "data/"
  var modelTimeInterval = 1000 * 60       // 1 mins

  def main(args: Array[String]) {

    println(s"Using kafka brokers at $KAFKA_BROKER")
    println(s"Data Message delay $dataTimeInterval")

    val kafka = KafkaLocalServer(true)
    kafka.start()

    println(s"Cluster created")

    publishData()
    publishModels()

    while(true)
      pause(600000)
  }

  private def publishData() : Future[Unit] = Future {

    println("Starting data publisher")
    val bos = new ByteArrayOutputStream()
    val sender = MessageSender(KAFKA_BROKER)
    val data = signal()
    println("Publishing data")
    while (true) {
      for( i <- 1 to 100) {
        val record = new CPUData(data.next(), data.getLabel(), "cpu")
        bos.reset()
        record.writeTo(bos)
        sender.writeValue(DATA_TOPIC, bos.toByteArray)
        pause(dataTimeInterval)
      }
      println("Published another 100 records")
    }
  }

  def publishModels() : Future[Unit] = Future {

    val sender = MessageSender(KAFKA_BROKER)
    val bos = new ByteArrayOutputStream()
    val files = getListOfModelFiles(directory)
    files.foreach(file => {
      val input = new DataInputStream(new FileInputStream(file))
      val length = input.readLong()
      val bytes = new Array[Byte](length.toInt)
      input.read(bytes)
      val name = input.readUTF()
      val description = input.readUTF()
      val dataType = input.readUTF()

      val dis = new DataInputStream(new ByteArrayInputStream(bytes))
      val plen = dis.readLong().toInt
      val p = new Array[Byte](plen)
      dis.read(p)
      val preprocessor = ModelPreprocessing.parseFrom(p)
      val glen = dis.readLong().toInt
      val g = new Array[Byte](glen)
      dis.read(g)
      val pRecord = ModelDescriptor(
        name,
        description,
        dataType,
        ModelDescriptor.ModelType.TENSORFLOW,
        Some(preprocessor)
      ).withData(ByteString.copyFrom(g))
      bos.reset()
      pRecord.writeTo(bos)
      sender.writeValue(MODELS_TOPIC, bos.toByteArray)
      println(s"Published Model $description")
      pause(modelTimeInterval)
    })
  }

  private def pause(timeInterval : Long): Unit = {
    try {
      Thread.sleep(timeInterval)
    } catch {
      case _: Throwable => // Ignore
    }
  }

  private def signal(timeLength: Int = 50000): MarkovChain = {

    val generator = new Random()

    val STM : Array[Array[Double]] = Array(Array(.9, .1), Array(.5, .5))
    val basesignal = new Noise("normal", 0.3,.15)
    val anomalysignal = new Noise("normal", 0.6,.1)
    val basenoise = new RandomPulses(generator.nextDouble() * timeLength/15.0,  Math.abs(generator.nextDouble()) * .01)
    val anomalynoise = new RandomPulses(generator.nextDouble() * timeLength/15.0, Math.abs(generator.nextDouble()) * .01)

    val base = new Model(Seq((basesignal, 1), (basenoise,.1)))
    val anomaly = new Model(Seq((anomalysignal, 1), (anomalynoise,.1)))

    new MarkovChain(STM, Seq(base, anomaly), 3)
  }

  private def getListOfModelFiles(dir: String): Seq[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.map(_.getAbsolutePath)
    } else {
      Seq.empty[String]
    }
  }
}