package com.lightbend.modelServer.model

import java.io.ByteArrayOutputStream

import com.lightbend.modelServer.model.tensorflow.TensorFlowModel
import com.lightbend.model.modeldescriptor.{ModelDescriptor, ModelPreprocessing}

import scala.util.Try

/**
 * Created by boris on 5/8/17.
 */
object ModelToServe {

  private val factories = Map(ModelDescriptor.ModelType.TENSORFLOW.value -> TensorFlowModel)

  def fromByteArray(message: Array[Byte]): Try[ModelToServe] = Try {
    val m = ModelDescriptor.parseFrom(message)
    m.messageContent.isData match {
      case true => new ModelToServe(m.name.replace(" ", "_"), m.description, m.dataType, m.modeltype, m.getData.toByteArray,
       m.getPreprocessing.width, m.getPreprocessing.mean, m.getPreprocessing.std, m.getPreprocessing.input, m.getPreprocessing.output)
      case _ => throw new Exception("Location based is not yet supported")
    }
  }

  def copy(from: Option[Model]): Option[Model] =
    from match {
      case Some(model) => Some(factories.get(model.getType.asInstanceOf[Int]).get.restore(model.toBytes()))
      case _ => None
    }

  def restore(t : Int, content : Array[Byte]): Option[Model] = Some(factories.get(t).get.restore(content))

  def toModel(model: ModelToServe): Option[Model] = {
    factories.get(model.modelType.value) match {
      case Some(factory) => Some(factory.create(model))
      case _ => None
    }
  }
}

case class ModelToServe(name: String, description: String, dataType: String, modelType: ModelDescriptor.ModelType, model: Array[Byte] = Array.empty,
                        width: Int = 0, mean : Double = .0, std : Double = .0, input : String = "", output : String = "")

case class DataPreprocessor(width: Int, mean : Double, std : Double, input : String, output : String)

object DataPreprocessor{
  val bos = new ByteArrayOutputStream()

  def toByteArray(p : DataPreprocessor) : Array[Byte] = {
    val pb = new ModelPreprocessing(p.width, p.mean, p.std, p.input, p.output)
    bos.reset()
    pb.writeTo(bos)
    bos.toByteArray
  }
  def fromByteArray(bytes : Array[Byte]) : DataPreprocessor = {
    val pb = ModelPreprocessing.parseFrom(bytes)
    DataPreprocessor(pb.width, pb.mean, pb.std, pb.input, pb.output)
  }
}


case class ServingResult(processed : Boolean, model : String = "", source : Int=0, result: Option[AnyVal]=None, duration: Long = 0l)

object ServingResult{
  def noModel = ServingResult(false)
  def apply(model : String, source : Int, result: Option[AnyVal], duration: Long): ServingResult = ServingResult(true, model, source, result, duration)
}

case class ModelWithType(isCurrent : Boolean, dataType: String, model: Option[Model])