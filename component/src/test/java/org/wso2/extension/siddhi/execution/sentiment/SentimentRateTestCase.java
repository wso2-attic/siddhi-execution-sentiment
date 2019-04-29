/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.sentiment;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases.
 */
public class SentimentRateTestCase {
    private static Logger logger = Logger.getLogger(SentimentRateTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("Sentiment Extension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "define stream inputStream (text string); ";
        String query = ("@info(name = 'query1') " + "from inputStream "
                + "select sentiment:getRate(text) as isRate " + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(3, inEvent.getData(0));
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(-3, inEvent.getData(0));
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(0, inEvent.getData(0));
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(-2, inEvent.getData(0));
                    }
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[] { "Trump is a good person." });
        inputHandler.send(new Object[] { "Trump is a bad person." });
        inputHandler.send(new Object[] { "Trump is a good person. Trump is a bad person" });
        inputHandler.send(new Object[] { "What is wrong with these people" });
        siddhiAppRuntime.shutdown();
    }
}
