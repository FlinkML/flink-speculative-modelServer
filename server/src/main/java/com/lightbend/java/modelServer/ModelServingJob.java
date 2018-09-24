package com.lightbend.java.modelServer;

import com.lightbend.java.modelServer.flinkprocessors.ModelProcessor;
import com.lightbend.java.modelServer.flinkprocessors.RouterProcessor;
import com.lightbend.java.modelServer.flinkprocessors.SpeculativeProcessor;
import com.lightbend.java.modelServer.model.DataConverter;
import com.lightbend.java.modelServer.model.ModelToServe;
import com.lightbend.java.modelServer.model.speculative.SpeculativeModel;
import com.lightbend.java.modelServer.model.speculative.SpeculativeRequest;
import com.lightbend.java.modelServer.model.speculative.SpeculativeServiceRequest;
import com.lightbend.java.modelServer.typeschema.ByteArraySchema;
import com.lightbend.kafka.configuration.java.ModelServingConfiguration;
import com.lightbend.model.Cpudata;
import com.lightbend.speculative.Speculativedescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Optional;
import java.util.Properties;

public class ModelServingJob {
    public static void main(String[] args) {
//    executeLocal();
        executeServer();
    }

    // Execute on the local Flink server - to test queariable state
    private static void  executeServer() {

        // We use a mini cluster here for sake of simplicity, because I don't want
        // to require a Flink installation to run this demo. Everything should be
        // contained in this JAR.

        int port = 6124;
        int parallelism = 2;

        Configuration config = new Configuration();
        config.setInteger(JobManagerOptions.PORT, port);
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);

        // In a non MiniCluster setup queryable state is enabled by default.
        config.setString(QueryableStateOptions.PROXY_PORT_RANGE, "9069");
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 2);
        config.setInteger(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS, 2);

        config.setString(QueryableStateOptions.SERVER_PORT_RANGE, "9067");
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 2);
        config.setInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS, 2);

        try {

            // Create a local Flink server
            LocalFlinkMiniCluster flinkCluster = new LocalFlinkMiniCluster(
                    config,
                    HighAvailabilityServicesUtils.createHighAvailabilityServices(
                            config,
                            Executors.directExecutor(),
                            HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION),
                    false);
            // Start server and create environment
            flinkCluster.start(true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", port);
            env.setParallelism(parallelism);
            // Build Graph
            buildGraph(env);
            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            // Submit to the server and wait for completion
            flinkCluster.submitJobAndWait(jobGraph, false);
        } catch (Throwable t){
            t.printStackTrace();
        }
    }

    // Execute locally in the environment
    private static void  executeLocal(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildGraph(env);
        System.out.println("[info] Job ID: " + env.getStreamGraph().getJobGraph().getJobID());
        try {
            env.execute();
        }
        catch (Throwable t){
            t.printStackTrace();
        }
    }

    // Build execution Graph
    private static void buildGraph(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        // Side outputs
        OutputTag<SpeculativeModel> modelTag = new OutputTag<SpeculativeModel>("speculative-model"){};
        OutputTag<SpeculativeRequest> dataTag = new OutputTag<SpeculativeRequest>("speculative-data"){};


        // configure Kafka consumer
        // Data
        Properties dataKafkaProps = new Properties();
        dataKafkaProps.setProperty("zookeeper.connect", ModelServingConfiguration.ZOOKEEPER_HOST);
        dataKafkaProps.setProperty("bootstrap.servers", ModelServingConfiguration.KAFKA_BROKER);
        dataKafkaProps.setProperty("group.id", ModelServingConfiguration.DATA_GROUP);
        // always read the Kafka topic from the current location
        dataKafkaProps.setProperty("auto.offset.reset", "latest");

        // Model
        Properties modelKafkaProps = new Properties();
        modelKafkaProps.setProperty("zookeeper.connect", ModelServingConfiguration.ZOOKEEPER_HOST);
        modelKafkaProps.setProperty("bootstrap.servers", ModelServingConfiguration.KAFKA_BROKER);
        modelKafkaProps.setProperty("group.id", ModelServingConfiguration.MODELS_GROUP);
        // always read the Kafka topic from the beginning
        modelKafkaProps.setProperty("auto.offset.reset", "earliest");

        // Speculative configuration
        Properties speculativeKafkaProps = new Properties();
        speculativeKafkaProps.setProperty("zookeeper.connect", ModelServingConfiguration.ZOOKEEPER_HOST);
        speculativeKafkaProps.setProperty("bootstrap.servers", ModelServingConfiguration.KAFKA_BROKER);
        speculativeKafkaProps.setProperty("group.id", ModelServingConfiguration.SPECULATIVE_GROUP);
        // always read the Kafka topic from the current location
        speculativeKafkaProps.setProperty("auto.offset.reset", "earliest");


        // create a Kafka consumers
        // Data
        FlinkKafkaConsumer011<byte[]> dataConsumer = new FlinkKafkaConsumer011<>(
                ModelServingConfiguration.DATA_TOPIC,
                new ByteArraySchema(),
                dataKafkaProps);

        // Model
        FlinkKafkaConsumer011<byte[]> modelConsumer = new FlinkKafkaConsumer011<>(
                ModelServingConfiguration.MODELS_TOPIC,
                new ByteArraySchema(),
                modelKafkaProps);

        // Speculative
        FlinkKafkaConsumer011<byte[]> speculativeConsumer = new FlinkKafkaConsumer011<>(
                ModelServingConfiguration.SPECULATIVE_TOPIC,
                new ByteArraySchema(),
                speculativeKafkaProps);


        // Create input data streams
        DataStream<byte[]> modelsStream = env.addSource(modelConsumer, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        DataStream<byte[]> dataStream = env.addSource(dataConsumer, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        DataStream<byte[]> speculativeStream = env.addSource(speculativeConsumer, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

        // Read data from streams
        DataStream<ModelToServe> models = modelsStream
                .flatMap((byte[] value, Collector<ModelToServe> out) -> {
                    Optional<ModelToServe> model = DataConverter.convertModel(value);
                    if (model.isPresent())
                        out.collect(model.get());
                    else
                        System.out.println("Failed to convert model input");
                }).returns(ModelToServe.class)
                .keyBy(model -> model.getDataType());


        DataStream<Cpudata.CPUData> data = dataStream
                .flatMap((byte[] value, Collector<Cpudata.CPUData> out) -> {
                    Optional<Cpudata.CPUData> record = DataConverter.convertData(value);
                    if (record.isPresent())
                        out.collect(record.get());
                    else
                        System.out.println("Failed to convert data input");
                }).returns(Cpudata.CPUData.class)
                .keyBy(record -> record.getDataType());

        DataStream<Speculativedescriptor.SpeculativeDescriptor> config = speculativeStream
                .flatMap((byte[] value, Collector<Speculativedescriptor.SpeculativeDescriptor> out) -> {
                    Optional<Speculativedescriptor.SpeculativeDescriptor> record = DataConverter.convertConfig(value);
                    if (record.isPresent())
                        out.collect(record.get());
                    else
                        System.out.println("Failed to convert data input");
                }).returns(Speculativedescriptor.SpeculativeDescriptor.class)
                .keyBy(record -> record.getDatatype());


        // Route models and data
        DataStream<SpeculativeServiceRequest> speculativeStart = data
                .connect(models)
                .process(new RouterProcessor());

        // Pickup side inputs and process them
        DataStream<SpeculativeModel> modelUpdate = ((SingleOutputStreamOperator<SpeculativeServiceRequest>) speculativeStart).getSideOutput(modelTag)
                .keyBy(record -> record.getDataModel());

        DataStream<SpeculativeRequest> dataProcess = ((SingleOutputStreamOperator<SpeculativeServiceRequest>) speculativeStart).getSideOutput(dataTag)
                .keyBy(record -> record.getDataModel());

        // Process individual model
        DataStream<SpeculativeServiceRequest> individualresult = dataProcess
                .connect(modelUpdate)
                .process(new ModelProcessor());


        // Run voting
        speculativeStart.union(individualresult)
                .keyBy(record -> record.getDataType())
                .connect(config)
                .process(new SpeculativeProcessor())
                .map(result -> {
                    if(result.isProcessed() && result.getResult().isPresent())
                        System.out.println("Using model " + result.getModel() + ". Calculated result " + result.getResult().get() + ", expected " + result.getSource() + " calculated in " + result.getDuration() + "ms");
                    else System.out.println("No model available - skipping");
                    return result;
                });
    }
}