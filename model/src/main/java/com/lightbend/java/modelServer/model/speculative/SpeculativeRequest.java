package com.lightbend.java.modelServer.model.speculative;

public class SpeculativeRequest {

    private String dataModel, dataType, GUID;
    private Object data;

    public SpeculativeRequest(String dataModel, String dataType, String GUID, Object data){
        this.dataModel = dataModel;
        this.dataType = dataType;
        this.GUID = GUID;
        this.data = data;
    }

    public String getDataModel() {
        return dataModel;
    }

    public String getDataType() {
        return dataType;
    }

    public String getGUID() {
        return GUID;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return "SpeculativeRequest{" +
                "dataModel='" + dataModel + '\'' +
                ", dataType='" + dataType + '\'' +
                ", GUID='" + GUID + '\'' +
                ", data=" + data +
                '}';
    }
}
