package com.lightbend.java.modelServer.flinkprocessors;

import com.lightbend.java.modelServer.model.ModelToServe;
import com.lightbend.java.modelServer.model.speculative.SpeculativeRequest;
import com.lightbend.java.modelServer.model.speculative.SpeculativeServiceRequest;
import com.lightbend.model.Cpudata;
import com.lightbend.java.modelServer.model.ServingResult;
import com.lightbend.java.modelServer.model.speculative.SpeculativeModel;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class RouterProcessor extends CoProcessFunction<Cpudata.CPUData, ModelToServe, SpeculativeServiceRequest> {

    // Side output tags
    OutputTag<SpeculativeModel> modelTag = new OutputTag<SpeculativeModel>("speculative-model"){};
    OutputTag<SpeculativeRequest> dataTag = new OutputTag<SpeculativeRequest>("speculative-data"){};

    // In Flink class instance is created not for key, but rater key groups
    // https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/state/state.html#keyed-state-and-operator-state
    // As a result, any key specific sate data has to be in the key specific state
    // Known models
    ValueState<List<String>> currentModels;

    @Override public void open(Configuration parameters) {

        ValueStateDescriptor<List<String>> currentModelsDesc = new ValueStateDescriptor<>(
                "modelList", // state name
                TypeInformation.of(new TypeHint<List<String>>() {}));   // type information
        currentModelsDesc.setQueryable("currentModels");                // Expose it for queryable state
        currentModels = getRuntimeContext().getState(currentModelsDesc);
    }

    @Override public void processElement1(Cpudata.CPUData record, Context ctx, Collector<SpeculativeServiceRequest> out) throws Exception {
        if(currentModels.value() == null)
            currentModels.update(new ArrayList<>());
        String GUID = UUID.randomUUID().toString();
        // See if we have any models for this
        List<String> models = currentModels.value();
        if(models.isEmpty()){
            out.collect(new SpeculativeServiceRequest(record.getDataType(), "", GUID, Optional.of(ServingResult.getEmpty()),0));
            return;
        }
        out.collect(new SpeculativeServiceRequest(record.getDataType(), "", GUID));
        for(String model : models){
            ctx.output(dataTag, new SpeculativeRequest(model, record.getDataType(), GUID, record));
        }
    }

    @Override public void processElement2(ModelToServe model, Context ctx, Collector<SpeculativeServiceRequest> out) throws Exception {

        if(currentModels.value() == null) currentModels.update(new ArrayList<>());

        System.out.println ("Router Processor new model - " + model.getDescription());

        // Update known models if necessary
        String dataModel = model.getDataType() + "-" + model.getName();
        List<String> models = currentModels.value();
        if(!models.contains(dataModel)){
            models.add(dataModel);
            currentModels.update(models);
        }
        // emit data to side output
        System.out.println("Emmiting model with the key - " + dataModel);
        ctx.output(modelTag, new SpeculativeModel(dataModel, model));
    }
}
