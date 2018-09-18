package com.lightbend.java.modelServer.model.speculative;

import java.util.List;

public class CurrentProcessing {

    private String dataType;
    private int models;
    private long start;
    private List<ServingResponse> results;

    public CurrentProcessing(String dataType, int models, long start, List<ServingResponse> results){
        this.dataType = dataType;
        this.models = models;
        this.start = start;
        this.results = results;
    }

    public String getDataType() {
        return dataType;
    }

    public int getModels() {
        return models;
    }

    public long getStart() {
        return start;
    }

    public List<ServingResponse> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return "CurrentProcessing{" +
                "dataType='" + dataType + '\'' +
                ", models=" + models +
                ", start=" + start +
                ", results=" + results +
                '}';
    }
}
