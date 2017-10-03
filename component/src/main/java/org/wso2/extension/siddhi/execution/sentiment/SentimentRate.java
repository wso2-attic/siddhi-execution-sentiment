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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Sentiment Rate Implementation.
 */
@Extension(
        name = "getRate",
        namespace = "sentiment",
        description = "This provides the sentiment value for a given string as per Affin word list",
        parameters = {
                @Parameter(name = "text",
                        description = "The input text for which the sentiment value should be derived.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "This returns the sentiment value for the provided string",
                type = {DataType.INT}),
        examples = @Example(description = "This returns the sentiment value for the given input string by referring " +
                "the Afinn word list. In this scenario, the output is 3 .",
                syntax = "getRate('George is a good person')")
)
public class SentimentRate extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentRate.class);
    private Map<String, Integer> affinWordMap;

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

    @Override
    public Map<String, Object> currentState() {
        // No need to maintain a state.
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        // No need to maintain a state
    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid no of arguments passed to sentiment:getRate() function, "
                            + "required 1, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("First parameter should be of type string. But found "
                    + attributeExpressionExecutors[0].getReturnType());
        }
        // Load affinwords.txt in to affinWordMap
        affinWordMap = new HashMap<>();
        String[] split;
        try {
            String[] wordBuckets = getWordsBuckets("affinwords.txt");
            for (String bucket : wordBuckets) {
                split = bucket.split(" ");
                affinWordMap.put(split[0].trim(), Integer.parseInt(split[split.length - 1].trim()));
            }
        } catch (IOException e) {
            LOGGER.error("Failed to load affinwords.txt file");
        }
    }

    @Override
    protected Object execute(Object[] data) {
        return null;
    }

    @Override
    protected Object execute(Object data) {
        int rank = 0;
        String[] words = data.toString().split("\\W+");
        for (String word : words) {
            if (affinWordMap.containsKey(word)) {
                rank += affinWordMap.get(word);
            }
        }
        return rank;
    }

    private String[] getWordsBuckets(String fileName) throws IOException {
        StringBuilder textChunk = new StringBuilder();
        InputStream in = getClass().getResourceAsStream("/" + fileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
        try {
            while ((line = reader.readLine()) != null) {
                textChunk.append(line).append("\n");
            }
            in.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage() , e);
        } finally {
            reader.close();
        }
        return textChunk.toString().split(",");
    }
}
