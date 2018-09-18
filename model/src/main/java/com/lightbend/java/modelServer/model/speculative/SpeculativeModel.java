package com.lightbend.java.modelServer.model.speculative;

import com.lightbend.java.modelServer.model.ModelToServe;

public class SpeculativeModel {

    private String dataModel;
    private ModelToServe model;

    public SpeculativeModel(String dataModel, ModelToServe model){
        this.dataModel = dataModel;
        this.model = model;
    }

    public String getDataModel() {
        return dataModel;
    }

    public ModelToServe getModel() {
        return model;
    }

    @Override
    public String toString() {
        return "SpeculativeModel{" +
                "dataModel='" + dataModel + '\'' +
                ", model=" + model +
                '}';
    }
}
