package com.github.smueller18.flink.serialization;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
public class SchemaRegistryKeyedSerializationSchema implements KeyedSerializationSchema<GenericKeyValueRecord> {

    private String topic;
    private VerifiableProperties vProps;

    private transient Encoder keyEncoder;
    private transient Encoder valueEncoder;

    public SchemaRegistryKeyedSerializationSchema(VerifiableProperties vProps) {
        this.vProps = vProps;
    }

    @Override
    public byte[] serializeKey(GenericKeyValueRecord genericKeyValueRecord) {


        if (this.keyEncoder == null)
            this.keyEncoder = new KafkaAvroKeyEncoder(this.vProps);

        return ((KafkaAvroKeyEncoder) this.keyEncoder).toBytes(genericKeyValueRecord);

    }

    @Override
    public byte[] serializeValue(GenericKeyValueRecord genericKeyValueRecord) {

        if (this.valueEncoder == null)
            this.valueEncoder = new KafkaAvroValueEncoder(vProps);

        return ((KafkaAvroValueEncoder) this.valueEncoder).toBytes(genericKeyValueRecord);

    }

    @Override
    public String getTargetTopic(GenericKeyValueRecord genericKeyValueRecord) {
        return null;
    }

}
