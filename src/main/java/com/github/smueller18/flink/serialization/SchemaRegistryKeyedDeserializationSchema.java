package com.github.smueller18.flink.serialization;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
public class SchemaRegistryKeyedDeserializationSchema implements KeyedDeserializationSchema<GenericKeyValueRecord> {

    private VerifiableProperties vProps;
    private transient KafkaAvroDecoder decoder;

    public SchemaRegistryKeyedDeserializationSchema(VerifiableProperties vProps) {
        this.vProps = vProps;
    }

    @Override
    public GenericKeyValueRecord deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
            throws IOException {

        if(decoder == null)
            decoder = new KafkaAvroDecoder(this.vProps);

        return new GenericKeyValueRecord(
                (GenericRecord) decoder.fromBytes(messageKey),
                (GenericRecord) decoder.fromBytes(message)
        );
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
