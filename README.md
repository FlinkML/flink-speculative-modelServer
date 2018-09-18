# Flink Speculative Model server

This is a simple implementation of speculative model serving using Flink
which is an extension of [basic model service implementation](https://github.com/FlinkML/flink-modelServer)
and is based on the the [blog post](https://developer.lightbend.com/blog/2018-05-24-speculative-model-serving/index.html)

The overall Flink implementation is presented below:
![overall implementation](diagramm/Flink%20Speculative.png)

The project contains the following modules:
   
**Client** - Small application reading generating signal and publishing three pregenerated models

**Data** - prebuild models.
   
**Model** - Code describing base model artifacts and its transformation.

**Protobufs** - Definitions of protobufs used throughout implementation.
   
**Query** - Small application demonstrating external access to a Flink queryable data, containing
current model state. It works only for ModelServingKeyedJob.

**Server** - Actual Flink implementation.

Both Scala and Java implementations are provided.