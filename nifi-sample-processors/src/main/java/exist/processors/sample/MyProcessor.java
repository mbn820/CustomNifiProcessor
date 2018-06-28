/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package exist.processors.sample;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"example", "custom processor"})
@CapabilityDescription("Generates new flowfile")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="None", description="None")})
@WritesAttributes({@WritesAttribute(attribute="filename", description="The generated flowfile will have this attribute " +
"with the corresponding value of 'newFlowFile.txt'")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
        .Builder().name("MY_PROPERTY")
        .displayName("My property")
        .description("Sample required property")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor ANOTHER_PROPERTY = new PropertyDescriptor
        .Builder().name("Another Property")
        .displayName("Another Prop")
        .description("Sample optional property")
        .build();

    public static final PropertyDescriptor FORCE_FAILURE_PROPERTY = new PropertyDescriptor
        .Builder().name("Force failure")
        .displayName("Force Failure")
        .description("Route base on value")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    public static final PropertyDescriptor NUMBER_OF_COPIES = new PropertyDescriptor
        .Builder().name("Number of Copies")
        .displayName("Number of Copies")
        .required(true)
        .defaultValue("50")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("SUCCESS")
        .description("Success goes here")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("FAILURE")
        .description("Failure goes here")
        .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MY_PROPERTY);
        descriptors.add(ANOTHER_PROPERTY);
        descriptors.add(FORCE_FAILURE_PROPERTY);
        descriptors.add(NUMBER_OF_COPIES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        for(int i = 0; i < context.getProperty(NUMBER_OF_COPIES).asInteger(); i++) {
            FlowFile copy = session.create();
            copy = session.putAttribute(copy, "filename", "newFlowFile_" + i + ".txt");
            session.transfer(copy, REL_SUCCESS);
        }
    }

}

// comment
