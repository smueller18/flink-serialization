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

import org.apache.avro.generic.GenericRecord;


public class GenericKeyValueRecord {

    private GenericRecord key;
    private GenericRecord value;

    public GenericKeyValueRecord(GenericRecord key, GenericRecord value) {
        this.key = key;
        this.value = value;
    }

    public GenericRecord getKey() {
        return key;
    }

    public GenericRecord getValue() {
        return value;
    }

    public Object getKey(String id) throws RuntimeException {
        try {
            return key.get(id);
        }
        catch(RuntimeException e){
            throw new RuntimeException("Error while trying to get element with id " + id + ". ", e);
        }
    }

    public Object getValue(String id) throws RuntimeException {

        if (value.get(id) == null)
            throw new NullPointerException("Element with id '" + id + "' is Null.");
        return value.get(id);
    }
}
