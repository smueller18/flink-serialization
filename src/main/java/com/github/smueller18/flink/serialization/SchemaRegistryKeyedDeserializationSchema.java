package com.github.smueller18.flink.serialization;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Properties;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */

public class SchemaRegistryKeyedDeserializationSchema implements KeyedDeserializationSchema<GenericKeyValueRecord> {

    private Properties props;
    private transient KafkaAvroDecoder decoder;

    /***
     *
     * @param props properties for {@link KafkaAvroDecoder}
     *              schema.registry.url ({@link String}):
     *                  Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas
     *              max.schemas.per.subject ({@link Integer}, default: 1000):
     *                  Maximum number of schemas to create or cache locally
     *
     */
    public SchemaRegistryKeyedDeserializationSchema(Properties props) {
        this.props = props;
    }

    @Override
    public GenericKeyValueRecord deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
            throws IOException {

        if(decoder == null)
            decoder = new KafkaAvroDecoder(new VerifiableProperties(this.props));

        try {
            return new GenericKeyValueRecord(
                    (GenericRecord) decoder.fromBytes(messageKey),
                    (GenericRecord) decoder.fromBytes(message)
            );
        }
        catch(Exception e) {
            if (e.getCause() instanceof ConnectException)
                throw new ConnectException(
                        String.format("Connection to schema registry '%s' could not be established.",
                                this.props.getProperty("schema.registry.url")
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
