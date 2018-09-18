package com.lightbend.java.modelServer.model.speculative;

public class ServingQualifier {
    private  String key, value;

    public ServingQualifier(String key, String value){
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ServingQualifier{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
