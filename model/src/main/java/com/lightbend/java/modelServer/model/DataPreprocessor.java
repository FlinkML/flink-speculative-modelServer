package com.lightbend.java.modelServer.model;


import com.lightbend.model.Modeldescriptor.ModelPreprocessing;

import java.io.ByteArrayOutputStream;
import java.util.Optional;

public class DataPreprocessor {

    int width;
    double mean;
    double std;
    String input;
    String output;

    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    public DataPreprocessor(int width, double mean, double std, String input, String output){
        this.width = width;
        this.mean = mean;
        this.std = std;
        this.input = new String(input);
        this.output = new String(output);
    }

    public static Optional<DataPreprocessor> fromByteArray (byte[] bytes){
        try {
            // Unmarshall record
            ModelPreprocessing pd = ModelPreprocessing.parseFrom(bytes);
            return Optional.of(new DataPreprocessor(pd.getWidth(), pd.getMean(), pd.getStd(), pd.getInput(), pd.getOutput()));
        } catch (Throwable t) {
            // Oops
            System.out.println("Exception parsing preprocessor record" + new String(bytes));
            t.printStackTrace();
            return Optional.empty();
        }
    }


    public byte[] toByteArray(){
        ModelPreprocessing pb =
                ModelPreprocessing.newBuilder().setWidth(width).setMean(mean).setStd(std).setInput(input).setOutput(output).build();
        bos.reset();
        try {
            pb.writeTo(bos);
            return bos.toByteArray();
        }
        catch (Throwable t){
            // Oops
            System.out.println("Exception marshalling preprocessor record");
            t.printStackTrace();
            return null;
        }
    }

    public int getWidth() {
        return width;
    }

    public double getMean() {
        return mean;
    }

    public double getStd() {
        return std;
    }

    public String getInput() {
        return input;
    }

    public String getOutput() {
        return output;
    }

}
