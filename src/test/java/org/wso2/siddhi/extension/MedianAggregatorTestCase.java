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

import org.apache.log4j.Logger;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class MedianAggregatorTestCase {

    private static final Logger log = Logger.getLogger(MedianAggregatorTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);


    @org.junit.Test
    public void Test1() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();


        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double);";

        String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select median(tt) as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                // EventPrinter.print(events);
                for(Event ev : events){
                    System.out.println("" + ev.getData()[0]);


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



        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }
/*
    @Test
    public void Test2() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        //siddhiManager.setExtension("median:median", MedianAggregator.class);
        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double); define stream filteredOutputStream (tt double);";
        //String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select medi
        // an:median(tt) as tt insert into outputStream;";
        String query ="@info(name = 'query1') " + "from inputStream#window.lengthBatch(5) " + "select median:med(tt) as tt insert into outputStream; from outputStream[tt != -1.0d] select tt as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                // EventPrinter.print(events);
                for(Event ev : events){
                    System.out.println("" + ev.getData()[0]);
                    ccc++;

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



        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void Test3() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        //siddhiManager.setExtension("median:median", MedianAggregator.class);
        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double); define stream filteredOutputStream (tt double);";
        //String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select medi
        // an:median(tt) as tt insert into outputStream;";
        String query ="@info(name = 'query1') " + "from inputStream#window.length(3) " + "select median:med(tt) as tt insert into outputStream; from outputStream[tt != -1.0d] select tt as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                // EventPrinter.print(events);
                for(Event ev : events){
                    System.out.println("" + ev.getData()[0]);
                    ccc++;

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



        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void Test4() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        //siddhiManager.setExtension("median:median", MedianAggregator.class);
        String inStreamDefinition = "define stream inputStream (tt double); define stream outputStream (tt double); define stream filteredOutputStream (tt double);";
        //String query ="@info(name = 'query1') " + "from inputStream#window.length(5) " + "select medi
        // an:median(tt) as tt insert into outputStream;";
        String query ="@info(name = 'query1') " + "from inputStream#window.lengthBatch(3) " + "select median:med(tt) as tt insert into outputStream; from outputStream[tt != -1.0d] select tt as tt insert into filteredOutputStream";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                // EventPrinter.print(events);
                for(Event ev : events){
                    System.out.println("" + ev.getData()[0]);
                    ccc++;

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



        Thread.sleep(2000);

        executionPlanRuntime.shutdown();
    }*/
}

