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
import java.util.HashMap;
import java.util.Map;

@Extension(
        name = "median",
        namespace = "stat",
        description = "Returns the median of aggregated events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be aggregated for the median.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.LONG})

        },
        returnAttributes = @ReturnAttribute(
                description = "Returns double for all data types. ie int, long, double and float",
                type = {DataType.DOUBLE}),
        examples = @Example(
                syntax = "from inputStream#window.length(5)" +
                        "\nselect stat:median(value) as medianOfValues" +
                        "\ninsert into outputStream;",
                description = "This will returns the median of aggregated values as a double value " +
                        "for each event arrival and expiry of sliding window length 5."
        )
)
public class MedianAggregator extends AttributeAggregator {
    private MedianAggregator medianAggregator;

    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                        ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Median aggregator has to have exactly 1 parameter, currently " +
                    expressionExecutors.length + " parameters are provided");
        }

        Attribute.Type type = expressionExecutors[0].getReturnType();

        switch (type) {
            case DOUBLE:
                this.medianAggregator = new MedianAggregatorDouble();
                break;
            case INT:
                this.medianAggregator = new MedianAggregatorInt();
                break;
            case FLOAT:
                this.medianAggregator = new MedianAggregatorFloat();
            case LONG:
                this.medianAggregator = new MedianAggregatorLong();
            default:
                throw new OperationNotSupportedException("Median not supported for " + type);
        }
    }


    public Attribute.Type getReturnType() {
        return Attribute.Type.DOUBLE;
    }

    public Object processAdd(Object data) {
        return this.medianAggregator.processAdd(data);
    }


    public Object processAdd(Object[] data) {
        return new IllegalStateException("Median cannot process data array, but found " + Arrays.deepToString(data));
    }


    public Object processRemove(Object data) {
        return this.medianAggregator.processRemove(data);
    }


    public Object processRemove(Object[] data) {
        return new IllegalStateException("Median cannot process data array, but found " + Arrays.deepToString(data));
    }


    public Object reset() {
        return this.medianAggregator.reset();
    }


    public void start() {
    }


    public void stop() {
    }

    public Map<String, Object> currentState() {
        return this.medianAggregator.currentState();
    }

    public void restoreState(Map<String, Object> map) {
        this.medianAggregator.restoreState(map);
    }


    private class MedianAggregatorDouble extends MedianAggregator {

        private ArrayList<Double> arrayList = new ArrayList<Double>();
        private int count = 0;
        private double median;


        public Object processAdd(Object data) {
            this.arrayList.add((Double) data);
            this.count++;
            this.median = getMedian();
            return median;
        }

        private double getMedian() {
            Collections.sort(this.arrayList);

            int midPointA = this.count / 2;
            if (this.count % 2 == 0) {
                int midPointB = midPointA - 1;
                return (this.arrayList.get(midPointA) + this.arrayList.get(midPointB)) / 2.0;
            }

            return this.arrayList.get(midPointA);
        }


        public Object processRemove(Object data) {
            this.arrayList.remove(data);
            this.count--;
            return median;
        }


        public Object reset() {
            this.arrayList = new ArrayList<Double>();
            this.count = 0;
            this.median = 0.0;
            return median;
        }

        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap();
            state.put("Median", Double.valueOf(this.median));
            state.put("Count", Integer.valueOf(this.count));
            return state;
        }

        public void restoreState(Map<String, Object> state) {
            this.median = ((Double) state.get("Median")).doubleValue();
            this.count = ((Integer) state.get("Count")).intValue();

        }
    }

    private class MedianAggregatorLong extends MedianAggregator {

        private ArrayList<Long> arrayList = new ArrayList<Long>();
        private int count = 0;
        private double median;


        public Object processAdd(Object data) {
            this.arrayList.add((Long) data);
            this.count++;
            this.median = getMedian();
            return median;
        }

        private double getMedian() {
            Collections.sort(this.arrayList);

            int midPointA = this.count / 2;
            if (this.count % 2 == 0) {
                int midPointB = midPointA - 1;
                return (this.arrayList.get(midPointA) + this.arrayList.get(midPointB)) / 2.0;
            }

            return this.arrayList.get(midPointA);
        }


        public Object processRemove(Object data) {
            this.arrayList.remove(data);
            this.count--;
            return median;
        }


        public Object reset() {
            this.arrayList = new ArrayList<Long>();
            this.count = 0;
            this.median = 0.0;
            return median;
        }

        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap();
            state.put("Median", Double.valueOf(this.median));
            state.put("Count", Integer.valueOf(this.count));
            return state;
        }

        public void restoreState(Map<String, Object> state) {
            this.median = ((Double) state.get("Median")).doubleValue();
            this.count = ((Integer) state.get("Count")).intValue();

        }
    }

    private class MedianAggregatorFloat extends MedianAggregator {

        private ArrayList<Float> arrayList = new ArrayList<Float>();
        private int count = 0;
        private double median;

        public Object processAdd(Object data) {
            this.arrayList.add((Float) data);
            this.count++;
            this.median = getMedian();
            return median;
        }

        private double getMedian() {
            Collections.sort(this.arrayList);

            int midPointA = this.count / 2;
            if (this.count % 2 == 0) {
                int midPointB = midPointA - 1;
                return (this.arrayList.get(midPointA) + this.arrayList.get(midPointB)) / 2.0;
            }

            return this.arrayList.get(midPointA);
        }

        public Object processRemove(Object data) {
            this.arrayList.remove(data);
            this.count--;
            return this.median;
        }


        public Object reset() {
            this.arrayList = new ArrayList<Float>();
            this.count = 0;
            this.median = 0.0;
            return this.median;
        }

        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap();
            state.put("Median", Double.valueOf(this.median));
            state.put("Count", Integer.valueOf(this.count));
            return state;
        }

        public void restoreState(Map<String, Object> state) {
            this.median = ((Double) state.get("Median")).doubleValue();
            this.count = ((Integer) state.get("Count")).intValue();

        }
    }

    private class MedianAggregatorInt extends MedianAggregator {

        private ArrayList<Integer> arrayList = new ArrayList<Integer>();
        private int count = 0;
        private double median;

        public Object processAdd(Object data) {
            this.arrayList.add((Integer) data);
            this.count++;
            this.median = getMedian();
            return median;
        }

        private Double getMedian() {

            Collections.sort(this.arrayList);

            int midPointA = this.count / 2;
            if (this.count % 2 == 0) {
                int midPointB = midPointA - 1;
                return (this.arrayList.get(midPointA) + this.arrayList.get(midPointB)) / 2.0;
            }

            return this.arrayList.get(midPointA) * 1.0;
        }

        public Object processRemove(Object data) {
            this.arrayList.remove(data);
            this.count--;
            return median;
        }


        public Object reset() {
            this.arrayList = new ArrayList<Integer>();
            this.count = 0;
            this.median = 0.0;
            return median;
        }

        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap();
            state.put("Median", Double.valueOf(this.median));
            state.put("Count", Integer.valueOf(this.count));
            return state;
        }

        public void restoreState(Map<String, Object> state) {
            this.median = ((Double) state.get("Median")).doubleValue();
            this.count = ((Integer) state.get("Count")).intValue();

        }
    }


}



