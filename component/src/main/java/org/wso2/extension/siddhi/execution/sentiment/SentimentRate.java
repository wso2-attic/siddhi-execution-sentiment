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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        description = "This provides the sentiment value for a given string as per the AFINN word list.",
        parameters = {
                @Parameter(name = "text",
                        description = "The input text for which the sentiment value should be derived.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "This returns the sentiment value for the provided string.",
                type = {DataType.INT}),
        examples = @Example(
                syntax = "getRate('George is a good person')",
                description = "This returns the sentiment value for the given input string by referring " +
                        "to the AFINN word list. In this scenario, the output is 3.")
)
public class SentimentRate extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentRate.class);
    private Map<String, Integer> affinWordMap;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
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
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null;
    }

    @Override
    protected Object execute(Object data, State state) {
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
