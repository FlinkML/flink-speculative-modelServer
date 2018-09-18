package com.lightbend.java.modelServer.query;

import com.lightbend.java.modelServer.model.ModelToServeStats;
import com.lightbend.java.modelServer.model.speculative.SpeculativeExecutionStats;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ModelStateQuery {

    private static final int timeInterval = 1000 * 20;        // 20 sec

    public static void main(String[] args) throws Exception {

        JobID jobId = JobID.fromHexString("93e731a1da73551656c459b58a264e62");
        List<String> types = Arrays.asList("cpu");

        QueryableStateClient client = new QueryableStateClient("127.0.0.1", 9069);

        // State descriptor for individual mpdels
        ValueStateDescriptor<List<String>> currentModelsDesc = new ValueStateDescriptor<List<String>>(
                "modelList", // state name
                TypeInformation.of(new TypeHint<List<String>>() {}).createSerializer(new ExecutionConfig())  // type Serializer
        );

        // State descriptor for voting model.
        ValueStateDescriptor<SpeculativeExecutionStats> speculativeModelDesc = new ValueStateDescriptor<>(
                "currentSpeculativeModel",   // state name
                TypeInformation.of(SpeculativeExecutionStats.class).createSerializer(new ExecutionConfig())  // type Serializer
        );

        // State descriptor for state of individual model.
        ValueStateDescriptor<ModelToServeStats> modelStateDesc = new ValueStateDescriptor<>(
                "currentModelState",                  // state name
                TypeInformation.of(ModelToServeStats.class).createSerializer(new ExecutionConfig()) // type serializer
        );

        BasicTypeInfo keyType = BasicTypeInfo.STRING_TYPE_INFO;

        while(true) {
            for (String key : types) {

                // Get model list
                CompletableFuture<ValueState<List<String>>> modelsListFuture = client.getKvState(jobId, "currentModels", key, keyType, currentModelsDesc);
                modelsListFuture.thenAccept(response -> {
                    try{
                        List<String> models = response.value();
                        System.out.println("Currrent models for type " + key + " are " + models);
                    }catch (Throwable e) {
                        e.printStackTrace();
                    }
                });
                List<String> models = modelsListFuture.get().value();

                // Individual models
                for (String model : models){
                    CompletableFuture<ValueState<ModelToServeStats>> modelStateFuture = client.getKvState(jobId, "currentModelState", model, keyType, modelStateDesc);
                    modelStateFuture.thenAccept(response -> {
                        try{
                            ModelToServeStats stats = response.value();
                            System.out.println("Model " + stats.getName() + " deployed at " + new DateTime(stats.getSince()).toString("yyyy/MM/dd HH:MM:SS") +
                                    ".  Average execution " + stats.getDuration()/stats.getDuration() + " min execution " + stats.getMin() + " max execution " + stats.getMax());
                        }catch (Throwable e) {
                            e.printStackTrace();
                        }
                    });
                }

                // Speculative model
                CompletableFuture<ValueState<SpeculativeExecutionStats>> speculativeStateFuture = client.getKvState(jobId, "speculativeState", key, keyType, speculativeModelDesc);
                speculativeStateFuture.thenAccept(response -> {
                    try {
                        SpeculativeExecutionStats stats = response.value();
                        System.out.println("Speculative Model for type " + key + " is " + stats.getName() + " deployed at " +
                                new DateTime(stats.getSince()).toString("yyyy/MM/dd HH:MM:SS") + " decider " + stats.getDecider() + " timeout " + stats.getTmout() +
                                ".  Average execution " + stats.getDuration()/stats.getDuration() + " min execution " + stats.getMin() + " max execution " + stats.getMax());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            Thread.sleep(timeInterval);
        }
    }
}