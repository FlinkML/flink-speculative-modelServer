package com.lightbend.java.modelServer.model.speculative;


import com.lightbend.java.modelServer.model.ServingResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ServingResponse {

    private  String GUID;
    private ServingResult result;
    Optional<Double> confidence = Optional.empty();
    List<ServingQualifier> qualifiers = new ArrayList();

    public ServingResponse(String GUID, ServingResult result){
        this.GUID = GUID;
        this.result = result;
    }

    public ServingResponse(String GUID, ServingResult result, Optional<Double> confidence){
        this.GUID = GUID;
        this.result = result;
        this.confidence = confidence;
    }

    public ServingResponse(String GUID, ServingResult result, List<ServingQualifier> qualifiers){
        this.GUID = GUID;
        this.result = result;
        this.qualifiers = qualifiers;
    }

    public ServingResponse(String GUID, ServingResult result, Optional<Double> confidence, List<ServingQualifier> qualifiers){
        this.GUID = GUID;
        this.result = result;
        this.confidence = confidence;
        this.qualifiers = qualifiers;
    }

    public String getGUID() {
        return GUID;
    }

    public ServingResult getResult() {
        return result;
    }

    public Optional<Double> getConfidence() {
        return confidence;
    }

    public List<ServingQualifier> getQualifiers() {
        return qualifiers;
    }

    @Override
    public String toString() {
        return "ServingResponse{" +
                "GUID='" + GUID + '\'' +
                ", result=" + result +
                ", confidence=" + confidence +
                ", qualifiers=" + qualifiers +
                '}';
    }
}
