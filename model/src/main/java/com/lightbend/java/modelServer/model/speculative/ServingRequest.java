package com.lightbend.java.modelServer.model.speculative;

public class ServingRequest {

    private String GUID;
    private Object data;

    public ServingRequest(String GUID, Object data){
        this.GUID = GUID;
        this.data = data;
    }

    public String getGUID() {
        return GUID;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return "ServingRequest{" +
                "GUID='" + GUID + '\'' +
                ", data=" + data +
                '}';
    }
}
