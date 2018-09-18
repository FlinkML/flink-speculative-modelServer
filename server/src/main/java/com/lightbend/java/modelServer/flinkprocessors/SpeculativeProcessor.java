package com.lightbend.java.modelServer.flinkprocessors;

import com.lightbend.java.modelServer.model.ServingResult;
import com.lightbend.java.modelServer.model.speculative.*;
import com.lightbend.java.modelServer.processor.VotingDecider;
import com.lightbend.speculative.Speculativedescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SpeculativeProcessor extends CoProcessFunction<SpeculativeServiceRequest, Speculativedescriptor.SpeculativeDescriptor, ServingResult> {

    private static final long SERVERTIMEOUT = 150l;

    Decider speculativeDecider = new VotingDecider();

    // The managed keyed state see https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/state.html
    ValueState<SpeculativeExecutionStats> specualtiveState;
    ValueState<Long> speculativeTimeout;

    // Because we are explicitely using unique GUIDs, we do not need to bring this to the key scope
    // Using this in the keygroup scope is good enough.
    Map<String, CurrentProcessing> currentProcessing = new HashMap<>();

    @Override public void open(Configuration parameters) {

        ValueStateDescriptor<SpeculativeExecutionStats> speculativeModelDesc = new ValueStateDescriptor<SpeculativeExecutionStats>(
                "currentSpeculativeModel",   // state name
                TypeInformation.of(new TypeHint<SpeculativeExecutionStats>() {})); // type information
        speculativeModelDesc.setQueryable("speculativeState");
        specualtiveState = getRuntimeContext().getState(speculativeModelDesc);
        ValueStateDescriptor<Long> speculativeTmoutDesc = new ValueStateDescriptor<Long> (
                "Timeout",
                TypeInformation.of(new TypeHint<Long>() {})); // type information
        speculativeTimeout = getRuntimeContext().getState(speculativeTmoutDesc);
    }


    @Override public void processElement1(SpeculativeServiceRequest record, Context ctx, Collector<ServingResult> out) throws Exception {

        if(specualtiveState.value() == null) specualtiveState.update(new SpeculativeExecutionStats(record.getDataType(), speculativeDecider.getClass().getName(), SERVERTIMEOUT));
        if(speculativeTimeout.value() == null) speculativeTimeout.update(SERVERTIMEOUT);

        // This will be invoked in 3 different use cases - process accordingly
        if(record.getResult().isPresent()){
            // This is either intermediate result from Model Processor or result from Router (no models)
            if (record.getModels() == 0){
                // from the router - just forward
                out.collect(record.getResult().get());
                return;
            }
            // Intermediate result
            CurrentProcessing results = currentProcessing.get(record.getGUID());
            if(results != null){
                // Still processing
                results.getResults().add(new ServingResponse(record.getGUID(), record.getResult().get()));
                if (results.getResults().size() >= results.getModels()){
                    // We are done
                    processResult(record.getGUID(), results, out);
                }
            }
            return;
        }
        // This is start collection
        // Add to the watch list
        long currentTime = System.currentTimeMillis();
        currentProcessing.put(record.getGUID(), new CurrentProcessing(record.getDataType(), record.getModels(), currentTime, new ArrayList<>()));
        // Schedule timeout
        ctx.timerService().registerProcessingTimeTimer(currentTime + speculativeTimeout.value());
     }

    @Override public void processElement2(Speculativedescriptor.SpeculativeDescriptor value, Context ctx, Collector<ServingResult> out) throws Exception {

        speculativeTimeout.update((value.getTmout()));
        if(specualtiveState.value() == null) specualtiveState.update(new SpeculativeExecutionStats(value.getDatatype(), speculativeDecider.getClass().getName(), value.getTmout()));
    }

    @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<ServingResult> out) {
        // Complete timed out requests
        try {

            long start = System.currentTimeMillis() - speculativeTimeout.value();
            for (Map.Entry entry : currentProcessing.entrySet()) {
                CurrentProcessing processing = (CurrentProcessing) entry.getValue();
                if (processing.getStart() < start) {
                    // Request timed out
                    System.out.println("Timed out request " + entry.getKey() + " at " + System.currentTimeMillis() + " started at " + processing.getStart());

                    processResult(entry.getKey().toString(), processing, out);
                }
            }
        }catch (Throwable t){}
    }

    // Complete speculative execution
    private void processResult(String GUID, CurrentProcessing results, Collector<ServingResult> out) throws Exception {
        // Run it through decider
        ServingResult servingResult = speculativeDecider.decideResult(results);
        // Return valye
        out.collect(servingResult);
        // Update state
        if(servingResult.isProcessed()) {
            SpeculativeExecutionStats state = specualtiveState.value();
            state.incrementUsage(servingResult.getDuration());
            specualtiveState.update(state);
        }
        // remove state
        currentProcessing.remove(GUID);
  }
}
