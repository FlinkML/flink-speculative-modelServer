package com.lightbend.java.modelServer.model.speculative;

public class SpeculativeExecutionStats {

    private static SpeculativeExecutionStats instance = null;

    private String name;
    private String decider;
    private long tmout;
    private long since;
    private long invocations;
    private double duration;
    private long min;
    private long max;

    public SpeculativeExecutionStats(){
        this("","",0);
    }

    public SpeculativeExecutionStats(final String name, final String decider, long tmout) {
        this.name = name;
        this.decider = decider;
        this.tmout = tmout;
        this.since = System.currentTimeMillis();
        this.invocations = 0;
        this.duration = 0.;
        this.min = Long.MAX_VALUE;
        this.max = Long.MIN_VALUE;
    }

    public void updateConfig(long timeout) {tmout = timeout;}

    public void incrementUsage(long execution){
        invocations++;
        duration += execution;
        if(execution < min) min = execution;
        if(execution > max) max = execution;
    }

    static public SpeculativeExecutionStats empty(){
        if(instance == null)
            instance = new SpeculativeExecutionStats();
        return instance;
    }

    public String getName() { return name; }

    public String getDecider() { return decider; }

    public long getTmout() { return tmout; }

    public long getSince() { return since; }

    public long getInvocations() { return invocations; }

    public double getDuration() { return duration; }

    public long getMin() { return min; }

    public long getMax() { return max; }

    @Override
    public String toString() {
        return "SpeculativeExecutionStats{" +
                "name='" + name + '\'' +
                ", decider='" + decider + '\'' +
                ", tmout=" + tmout +
                ", since=" + since +
                ", invocations=" + invocations +
                ", duration=" + duration +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}