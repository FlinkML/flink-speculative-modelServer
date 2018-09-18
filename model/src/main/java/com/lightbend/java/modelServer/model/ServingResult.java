package com.lightbend.java.modelServer.model;

import java.util.Optional;

public class ServingResult {
    private boolean processed;
    private String model = "";
    private int source = 0;
    private Optional<Object> result = Optional.empty();
    long duration = 0l;

    private static ServingResult empty = new ServingResult();

    public ServingResult(){
        this.processed = false;
    }

    public ServingResult(String model, int source, Optional<Object> result, long duration){
        this.processed = true;
        this.model = model;
        this.source = source;
        this.result = result;
        this.duration = duration;
    }

    public boolean isProcessed() {
        return processed;
    }

    public String getModel() {
        return model;
    }

    public int getSource() {
        return source;
    }

    public Optional<Object> getResult() {
        return result;
    }

    public long getDuration() {
        return duration;
    }

    @Override
    public String toString() {
        return "ServingResult{" +
                "processed=" + processed +
                ", model='" + model + '\'' +
                ", source=" + source +
                ", result=" + result +
                ", duration=" + duration +
                '}';
    }

    public static ServingResult getEmpty() {return empty;}
}
