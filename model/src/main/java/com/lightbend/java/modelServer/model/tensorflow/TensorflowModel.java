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

package com.lightbend.java.modelServer.model.tensorflow;

/**
 * Created by boris on 5/26/17.
 */

import com.lightbend.java.modelServer.model.Model;
import com.lightbend.model.Cpudata;
import com.lightbend.model.Modeldescriptor;
import com.lightbend.java.modelServer.model.DataPreprocessor;
import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.util.Optional;


public class TensorflowModel implements Model {
    private Graph graph = new Graph();
    private Session session;
    private Preprocessor preprocessor;
    private DataPreprocessor dp;

    public TensorflowModel(byte[] inputStream, DataPreprocessor dp) {
        this.dp = dp;
        preprocessor = new Preprocessor(dp.getWidth(), dp.getMean(), dp.getStd());
        graph.importGraphDef(inputStream);
        session = new Session(graph);
    }

    @Override
    public Object score(Object input) {
        Cpudata.CPUData record = (Cpudata.CPUData) input;
        preprocessor.addMeasurement(record.getUtilization());
        if(preprocessor.getCurrentWidth() >= dp.getWidth()) {
            float[][] tmatrix = new float[1][];
            tmatrix[0] = preprocessor.getValue();
            Tensor tinput = Tensor.create(tmatrix);
            Tensor result = session.runner().feed(dp.getInput(), tinput).fetch(dp.getOutput()).run().get(0);
            long[] rshape = result.shape();
            float[][] rArray = new float[(int)rshape[0]][(int)rshape[1]];
            result.copyTo(rArray);
            float[] softmax = rArray[0];
            if (softmax[0] >= softmax[1]) return Optional.of(0); else return Optional.of(1);
        }
        else return Optional.empty();
    }

    @Override
    public void cleanup() {
        session.close();
        graph.close();
    }

    @Override
    public byte[] getBytes() {
        return graph.toGraphDef();
    }

    @Override
    public long getType() {
        return (long) Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber();
    }

    public Graph getGraph() {
        return graph;
    }

    private class Preprocessor {

        int width;
        double mean;
        double std;
        int currentWidth = 0;
        float[] value;

        public Preprocessor(int width, double mean, double std) {
            this.width = width;
            this.mean = mean;
            this.std = std;
            value = new float[width];
        }

        public int getCurrentWidth() { return currentWidth;}

        public void addMeasurement(double v) {

            for(int i = 1; i < (width - 1); i++){ value[i - 1] = value[i]; }
            value[width -1] = standardize(v);
            if (currentWidth < width) currentWidth = currentWidth + 1;
        }

        public float[] getValue(){ return value;}

        public float standardize(double value) { return (float) ((value - mean) / std); }
    }
}