package com.lightbend.java.modelServer.model.speculative;

import com.lightbend.java.modelServer.model.ServingResult;

import java.util.Optional;

public class SpeculativeServiceRequest {

    private String dataType, dataModel, GUID;
    private Optional<ServingResult> result = Optional.empty();
    private int models = 0;

    public SpeculativeServiceRequest(String dataType, String dataModel, String GUID){
        this.dataModel = dataModel;
        this.dataType = dataType;
        this.GUID = GUID;
    }

    public SpeculativeServiceRequest(String dataType, String dataModel, String GUID, Optional<ServingResult> result, int models){
        this.dataModel = dataModel;
        this.dataType = dataType;
        this.GUID = GUID;
        this.result = result;
        this.models = models;
    }

    public String getDataType() {
        return dataType;
    }

    public String getDataModel() {
        return dataModel;
    }

    public String getGUID() {
        return GUID;
    }

    public Optional<ServingResult> getResult() {
        return result;
    }

    public int getModels() {
        return models;
    }

    @Override
    public String toString() {
        return "SpeculativeServiceRequest{" +
                "dataType='" + dataType + '\'' +
                ", dataModel='" + dataModel + '\'' +
                ", GUID='" + GUID + '\'' +
                ", result=" + result +
                ", models=" + models +
                '}';
    }
}
