package com.lightbend.java.modelServer.processor;

import com.lightbend.java.modelServer.model.ServingResult;
import com.lightbend.java.modelServer.model.speculative.CurrentProcessing;
import com.lightbend.java.modelServer.model.speculative.Decider;
import com.lightbend.java.modelServer.model.speculative.ServingResponse;

import java.util.Optional;

public class VotingDecider implements Decider {

    @Override public ServingResult decideResult(CurrentProcessing results) {
        ServingResult result = ServingResult.getEmpty();
        double sum = .0;
        int count = 0;
        int source = 0;
        for (ServingResponse res : results.getResults()) {
            if (res.getResult().isProcessed()) {
                sum += (int) (res.getResult().getResult().orElse(0));
                if (res.getResult().getSource() != source) source = res.getResult().getSource();
                count += 1;
            }
        }
        if (count == 0)
            return result;
        int res = (sum / count < .5) ? 0 : 1;
        return new ServingResult("voter model", source, Optional.of(res), System.currentTimeMillis() - results.getStart());
    }
}
