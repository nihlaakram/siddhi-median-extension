/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.wso2.siddhi.extension.median;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@Extension(
        name = "median",
        namespace = "median",
        description = "TBD",
        parameters = {
                @Parameter(name = "data",
                        description = "The batch time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE}),

        },
        returnAttributes = @ReturnAttribute(
                description = "Returns median of aggregated events",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE}),
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class MedianAggregator extends AttributeAggregator {
    private MedianAggregator medianAgg;
    Logger log = Logger.getLogger(MedianAggregator.class);
    private int count = 0;


    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, ExecutionPlanContext executionPlanContext) {
        //(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Median aggregator has to have exactly 1 parameter, currently " +
                    expressionExecutors.length + " parameters provided");
        }

        Attribute.Type type = expressionExecutors[0].getReturnType();

        switch (type) {

            case DOUBLE:
                medianAgg = new MedianAggregatorDouble();
                break;
            case INT:
                medianAgg = new MedianAggregatorInt();
                break;
            case FLOAT:
                medianAgg = new MedianAggregatorFloat();
            default:
                throw new OperationNotSupportedException("Median not supported for " + type);
        }
    }


    public Attribute.Type getReturnType() {
        return medianAgg.getReturnType();
    }

    public Object processAdd(Object data) {
        return medianAgg.processAdd(data);
    }


    public Object processAdd(Object[] data) {
        return new IllegalStateException("Median cannot process data array, but found " + Arrays.deepToString(data));
    }


    public Object processRemove(Object data) {
        return medianAgg.processRemove(data);
    }


    public Object processRemove(Object[] data) {
        return new IllegalStateException("Median cannot process data array, but found " + Arrays.deepToString(data));
    }


    public Object reset() {
        return medianAgg.reset();
    }


    public void start() {
    }


    public void stop() {
    }

    public Map<String, Object> currentState() {
        return null;
    }

    public void restoreState(Map<String, Object> map) {

    }


    private class MedianAggregatorDouble extends MedianAggregator {
        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private ArrayList<Double> arr = new ArrayList<Double>();


        public Attribute.Type getReturnType() {
            return type;
        }


        public Object processAdd(Object data) {
            arr.add((Double) data);
            count++;
            return getMedian(arr);
        }

        private double getMedian(ArrayList<Double> test) {
            Collections.sort(test);

            int pointA = count / 2;
            if (count % 2 == 0) {
                int pointB = pointA - 1;
                return (test.get(pointA) + test.get(pointB)) / 2;
            }

            return test.get(pointA);
        }


        public Object processRemove(Object data) {
            arr.remove(data);
            count--;
            return 0.0;
        }


        public Object reset() {
            arr = new ArrayList<Double>();
            count = 0;
            return 0.0;
        }

        public Map<String, Object> currentState() {
            return null;
        }

        public void restoreState(Map<String, Object> map) {

        }
    }

    private class MedianAggregatorFloat extends MedianAggregator {
        private final Attribute.Type type = Attribute.Type.FLOAT;
        private ArrayList<Float> arr = new ArrayList<Float>();

        public Attribute.Type getReturnType() {
            return type;
        }


        public Object processAdd(Object data) {
            arr.add((Float) data);
            count++;
            return getMedian(arr);
        }

        private float getMedian(ArrayList<Float> test) {
            Collections.sort(test);

            int pointA = count / 2;
            if (count % 2 == 0) {
                int pointB = pointA - 1;
                return (test.get(pointA) + test.get(pointB)) / 2;
            }


            return test.get(pointA);
        }

        public Object processRemove(Object data) {
            arr.remove(data);
            count--;
            return 0.0;
        }


        public Object reset() {
            arr = new ArrayList<Float>();
            count = 0;
            return 0.0;
        }

        public Map<String, Object> currentState() {
            return null;
        }

        public void restoreState(Map<String, Object> map) {

        }
    }

    private class MedianAggregatorInt extends MedianAggregator {
        private final Attribute.Type type = Attribute.Type.INT;
        private ArrayList<Integer> arr = new ArrayList<Integer>();


        public Attribute.Type getReturnType() {
            return type;
        }

        public Object processAdd(Object data) {
            arr.add((Integer) data);
            count++;
            return getMedian(arr);
        }

        private int getMedian(ArrayList<Integer> test) {

            Collections.sort(test);

            int pointA = count / 2;
            if (count % 2 == 0) {
                int pointB = pointA - 1;
                return (test.get(pointA) + test.get(pointB)) / 2;
            }


            return test.get(pointA);
        }

        public Object processRemove(Object data) {
            arr.remove(data);
            count--;
            return 0.0;
        }


        public Object reset() {
            arr = new ArrayList<Integer>();
            count = 0;
            return 0.0;
        }

        public Map<String, Object> currentState() {
            return null;
        }

        public void restoreState(Map<String, Object> map) {

        }
    }


}



