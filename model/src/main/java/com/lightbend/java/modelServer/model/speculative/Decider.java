package com.lightbend.java.modelServer.model.speculative;

import com.lightbend.java.modelServer.model.ServingResult;

import java.io.Serializable;

public interface Decider extends Serializable {
    ServingResult decideResult(CurrentProcessing results);
}