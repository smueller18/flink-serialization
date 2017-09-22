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

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;


public class SchemaRegistryKeyedDeserializationSchema implements KeyedDeserializationSchema<GenericKeyValueRecord> {

    private Map<String, ?> configs;
    private transient KafkaAvroDeserializer keyDeserializer;
    private transient KafkaAvroDeserializer valueDeserializer;

    /***
     *
     * @param configs key-value-pairs for {@link KafkaAvroDeserializer}
     *              schema.registry.url ({@link String}):
     *                  Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas
     *              max.schemas.per.subject ({@link Integer}, default: 1000):
     *                  Maximum number of schemas to create or cache locally
     *
     */
    public SchemaRegistryKeyedDeserializationSchema(Map<String, ?> configs) {
        this.configs = configs;
    }

    @Override
    public GenericKeyValueRecord deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
            throws IOException {

        if(this.keyDeserializer == null) {
            this.keyDeserializer = new KafkaAvroDeserializer();
            this.keyDeserializer.configure(this.configs, true);
        }

        if(this.valueDeserializer == null) {
            this.valueDeserializer = new KafkaAvroDeserializer();
            this.valueDeserializer.configure(this.configs, false);
        }

        try {
            return new GenericKeyValueRecord(
                    (GenericRecord) keyDeserializer.deserialize(null, messageKey),
                    (GenericRecord) valueDeserializer.deserialize(null, message)
            );
        }
        catch(Exception e) {
            if (e.getCause() instanceof ConnectException)
                throw new ConnectException(
                        String.format("Connection to schema registry '%s' could not be established.",
                                this.configs.get("schema.registry.url")
                        )
                );
            else throw e;
        }
    }

    @Override
    public boolean isEndOfStream(GenericKeyValueRecord genericKeyValueRecord) {
        return false;
    }

    @Override
    public TypeInformation<GenericKeyValueRecord> getProducedType() {
        return TypeExtractor.getForClass(GenericKeyValueRecord.class);
    }
}
