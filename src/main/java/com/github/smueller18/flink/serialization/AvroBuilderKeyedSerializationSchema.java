/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Stephan Müller
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */


package com.github.smueller18.flink.serialization;

import java.util.Map;

import com.github.smueller18.avro.builder.AvroBuilder;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;


public class AvroBuilderKeyedSerializationSchema<T extends AvroBuilder> implements KeyedSerializationSchema<T> {

    private Map<String, ?> configs;

    private transient KafkaAvroSerializer keySerializer;
    private transient KafkaAvroSerializer valueSerializer;

    /***
     *
     * @param configs key-value-pairs for {@link KafkaAvroSerializer}
     *              schema.registry.url ({@link String}):
     *                  Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas
     *              max.schemas.per.subject ({@link Integer}, default: 1000):
     *                  Maximum number of schemas to create or cache locally
     *
     */
    public AvroBuilderKeyedSerializationSchema(Map<String, ?> configs) {
        this.configs = configs;
    }

    @Override
    public byte[] serializeKey(T avroBuilderObject) {

        if (this.keySerializer == null) {
            this.keySerializer = new KafkaAvroSerializer();
            this.keySerializer.configure(this.configs, true);
        }

        return this.keySerializer.serialize(AvroBuilder.getTopicName(avroBuilderObject.getClass()), avroBuilderObject.getKeyRecord());
    }

    @Override
    public byte[] serializeValue(T avroBuilderObject) {

        if (this.valueSerializer == null) {
            this.valueSerializer = new KafkaAvroSerializer();
            this.valueSerializer.configure(this.configs, false);
        }
        return this.valueSerializer.serialize(AvroBuilder.getTopicName(avroBuilderObject.getClass()), avroBuilderObject.getValueRecord());
    }

    @Override
    public String getTargetTopic(T avroBuilderObject) {
        return null;
    }

}
