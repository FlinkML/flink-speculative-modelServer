package com.lightbend.modelServer.model.speculative

import com.lightbend.modelServer.model.{ModelToServe, ServingResult}
import com.lightbend.speculative.speculativedescriptor.SpeculativeDescriptor

import scala.util.Try


case class ServingRequest(GUID : String, data : AnyVal)

case class ServingQualifier(key : String, value : String)

case class ServingResponse(GUID : String, result : ServingResult, confidence : Option[Double] = None, qualifiers : List[ServingQualifier] = List.empty)

case class SpeculativeRequest(dataModel : String, dataType : String, GUID: String, data: AnyVal)

case class SpeculativeModel(dataModel : String, model : ModelToServe)

case class SpeculativeServiceRequest(dataType : String, dataModel : String, GUID: String, result : Option[ServingResult], models: Int = 0)

object SpeculativeConverter {
  def fromByteArray(message: Array[Byte]): Try[SpeculativeDescriptor] = Try {
    SpeculativeDescriptor.parseFrom(message)
  }
}