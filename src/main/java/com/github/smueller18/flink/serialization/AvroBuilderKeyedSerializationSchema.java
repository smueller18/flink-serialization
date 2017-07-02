package com.github.smueller18.flink.serialization;

import com.github.smueller18.avro.builder.AvroBuilder;
import kafka.utils.VerifiableProperties;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Properties;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
public class AvroBuilderKeyedSerializationSchema<T extends AvroBuilder> implements KeyedSerializationSchema<T> {

    private Properties props;

    private transient KafkaAvroKeyEncoder keyEncoder;
    private transient KafkaAvroValueEncoder valueEncoder;

    /***
     *
     * @param props properties for {@link KafkaAvroKeyEncoder} and {@link KafkaAvroValueEncoder}
     *              schema.registry.url ({@link String}):
     *                  Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas
     *              max.schemas.per.subject ({@link Integer}, default: 1000):
     *                  Maximum number of schemas to create or cache locally
     *
     */
    public AvroBuilderKeyedSerializationSchema(Properties props) {
        this.props = props;
    }

    @Override
    public byte[] serializeKey(T avroBuilder) {

        if (this.keyEncoder == null)
            this.keyEncoder = new KafkaAvroKeyEncoder(new VerifiableProperties(this.props));

        return this.keyEncoder.toBytes(avroBuilder.getKeyRecord());
    }

    @Override
    public byte[] serializeValue(T avroBuilder) {

        if (this.valueEncoder == null)
            this.valueEncoder = new KafkaAvroValueEncoder(new VerifiableProperties(this.props));

        return this.valueEncoder.toBytes(avroBuilder.getValueRecord());
    }

    @Override
    public String getTargetTopic(T avroBuilder) {
        return null;
    }

}
