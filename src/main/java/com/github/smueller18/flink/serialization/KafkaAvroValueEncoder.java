package com.github.smueller18.flink.serialization;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.errors.SerializationException;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */

public class KafkaAvroValueEncoder extends AbstractKafkaAvroSerializer implements Encoder<Object> {

    public KafkaAvroValueEncoder(VerifiableProperties props) {
        configure(serializerConfig(props));
    }

    @Override
    public byte[] toBytes(Object object) {
        if (object instanceof GenericContainer)
            return serializeImpl(getSubjectName(((GenericContainer) object).getSchema().getFullName(), false), object);
        else
            throw new SerializationException("Primitive types are not supported yet");
    }
}
