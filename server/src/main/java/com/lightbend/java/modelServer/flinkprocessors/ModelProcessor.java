/*
 * Copyright (C) 2017  Lightbend
 *
 * This file is part of flink-ModelServing
 *
 * flink-ModelServing is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.lightbend.java.modelServer.flinkprocessors;

import com.lightbend.java.modelServer.model.DataConverter;
import com.lightbend.java.modelServer.model.Model;
import com.lightbend.java.modelServer.model.ModelToServeStats;
import com.lightbend.java.modelServer.model.ServingResult;
import com.lightbend.java.modelServer.model.speculative.SpeculativeModel;
import com.lightbend.java.modelServer.model.speculative.SpeculativeRequest;
import com.lightbend.java.modelServer.model.speculative.SpeculativeServiceRequest;
import com.lightbend.java.modelServer.typeschema.ModelTypeSerializer;
import com.lightbend.model.Cpudata;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class ModelProcessor extends CoProcessFunction<SpeculativeRequest, SpeculativeModel, SpeculativeServiceRequest>{

    ValueState<ModelToServeStats> modelState;
    ValueState<ModelToServeStats> newModelState;

    ValueState<Model> currentModel;
    ValueState<Model> newModel;


    @Override public void open(Configuration parameters){
        ValueStateDescriptor<ModelToServeStats> modeStatelDesc = new ValueStateDescriptor<>(
                "currentModelState",   // state name
                TypeInformation.of(new TypeHint<ModelToServeStats>() {})); // type information
        modeStatelDesc.setQueryable("currentModelState");
        modelState = getRuntimeContext().getState(modeStatelDesc);

        ValueStateDescriptor<ModelToServeStats> newModelStateDesc = new ValueStateDescriptor<>(
                "newModelState",         // state name
                TypeInformation.of(new TypeHint<ModelToServeStats>() {})); // type information
        newModelState = getRuntimeContext().getState(newModelStateDesc);

        ValueStateDescriptor<Model> currentModelDesc = new ValueStateDescriptor<>(
                "currentModel",         // state name
                new ModelTypeSerializer()); // type information
        currentModel = getRuntimeContext().getState(currentModelDesc);
        ValueStateDescriptor<Model> newModelDesc = new ValueStateDescriptor<>(
                "newModel",         // state name
                new ModelTypeSerializer()); // type information
        newModel = getRuntimeContext().getState(newModelDesc);
    }

    @Override public void processElement1(SpeculativeRequest record, Context ctx, Collector<SpeculativeServiceRequest> out) throws Exception {

        // See if we have update for the model
        if (newModel.value() != null) {
            // Clean up current model
            if (currentModel.value() != null)
                currentModel.value().cleanup();
            // Update model
            currentModel.update(newModel.value());
            modelState.update(newModelState.value());
            newModel.update(null);
        }
        // Process data
        if (currentModel.value() != null) {
//            System.out.println("Processing data " + record.getData());
            long start = System.currentTimeMillis();
            Object result = currentModel.value().score(record.getData());
            long duration = System.currentTimeMillis() - start;
            modelState.update(modelState.value().incrementUsage(duration));
            Cpudata.CPUData cpudata = (Cpudata.CPUData) record.getData();
            out.collect(new SpeculativeServiceRequest(cpudata.getDataType(), record.getDataType(), record.getGUID(),
                    Optional.of(new ServingResult(modelState.value().getDescription(), cpudata.getClass_(), (Optional<Object>) result, duration)), 1));
        }
    }

    @Override
    public void processElement2(SpeculativeModel model, Context ctx, Collector<SpeculativeServiceRequest> out) throws Exception {

        System.out.println("New model - " + model.getModel().getDescription());
        newModelState.update(new ModelToServeStats(model.getModel()));
        newModel.update(DataConverter.toModel(model.getModel()).orElse(null));
    }
}