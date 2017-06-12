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
package org.wso2.siddhi.extension;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class MedianAggregatorTestCase {

    private static final Logger log = Logger.getLogger(MedianAggregatorTestCase.class);
    private int count = 0;


    @org.junit.Test
    public void Test1() throws InterruptedException {

        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();


        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " + "select median:median(tt) as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                // EventPrinter.print(events);
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(8.94775, ev.getData()[0]);
                            break;
                        case 2:
                            Assert.assertEquals(8.81493, ev.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 4:
                            Assert.assertEquals(8.56327, ev.getData()[0]);
                            break;
                        case 5:
                            Assert.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 6:
                            Assert.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 7:
                            Assert.assertEquals(9.76563, ev.getData()[0]);
                            break;
                        case 8:
                            Assert.assertEquals(9.76563, ev.getData()[0]);
                            break;
                        case 9:
                            Assert.assertEquals(9.76563, ev.getData()[0]);
                            break;
                        case 10:
                            Assert.assertEquals(9.17144, ev.getData()[0]);
                            break;

                    }


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{8.94775});
        inputHandler.send(new Object[]{8.68211});
        inputHandler.send(new Object[]{8.44443});
        inputHandler.send(new Object[]{8.23472});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});


        executionPlanRuntime.shutdown();
    }

    @org.junit.Test
    public void Test2() throws InterruptedException {

        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();


        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.lengthBatch(5) " +
                "select median:median(tt) as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                // EventPrinter.print(events);
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 2:
                            Assert.assertEquals(9.17144, ev.getData(0));
                            break;


                    }


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{8.94775});
        inputHandler.send(new Object[]{8.68211});
        inputHandler.send(new Object[]{8.44443});
        inputHandler.send(new Object[]{8.23472});
        inputHandler.send(new Object[]{10.9959});
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});


        executionPlanRuntime.shutdown();
    }

    @org.junit.Test
    public void Test3() throws InterruptedException {

        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();


        String inStreamDefinition = "define stream inputStream (tt int); define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.lengthBatch(5) " +
                "select median:median(tt) as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                // EventPrinter.print(events);
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(3.0, ev.getData()[0]);
                            break;
                        case 2:
                            Assert.assertEquals(8.0, ev.getData(0));
                            break;


                    }


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{1});
        inputHandler.send(new Object[]{2});
        inputHandler.send(new Object[]{3});
        inputHandler.send(new Object[]{4});
        inputHandler.send(new Object[]{5});
        inputHandler.send(new Object[]{6});
        inputHandler.send(new Object[]{8});
        inputHandler.send(new Object[]{9});
        inputHandler.send(new Object[]{10});
        inputHandler.send(new Object[]{7});


        executionPlanRuntime.shutdown();
    }

    @org.junit.Test
    public void Test4() throws InterruptedException {

        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();


        String inStreamDefinition = "define stream inputStream (tt int); define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select median:median(tt) as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                // EventPrinter.print(events);
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(1.0, ev.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(1.5, ev.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(2.0, ev.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(2.5, ev.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(3.0, ev.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(4.0, ev.getData(0));
                            break;
                        case 7:
                            Assert.assertEquals(5.0, ev.getData(0));
                            break;
                        case 8:
                            Assert.assertEquals(6.0, ev.getData(0));
                            break;
                        case 9:
                            Assert.assertEquals(8.0, ev.getData(0));
                            break;
                        case 10:
                            Assert.assertEquals(8.0, ev.getData(0));
                            break;


                    }


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{1});
        inputHandler.send(new Object[]{2});
        inputHandler.send(new Object[]{3});
        inputHandler.send(new Object[]{4});
        inputHandler.send(new Object[]{5});
        inputHandler.send(new Object[]{6});
        inputHandler.send(new Object[]{8});
        inputHandler.send(new Object[]{9});
        inputHandler.send(new Object[]{10});
        inputHandler.send(new Object[]{7});


        executionPlanRuntime.shutdown();
    }
}
