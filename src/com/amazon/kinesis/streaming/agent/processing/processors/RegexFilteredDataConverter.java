package com.amazon.kinesis.streaming.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;


public class RegexFilteredDataConverter implements IDataConverter {

    private Pattern logEntryPattern;

    public RegexFilteredDataConverter(String matchPattern) {
        if (matchPattern != null) {
            this.logEntryPattern = Pattern.compile(matchPattern);
        } else {
            this.logEntryPattern = null;
        }
    }

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        String record = ByteBuffers.toString(data, StandardCharsets.UTF_8);
        Matcher matcher = logEntryPattern != null ? logEntryPattern.matcher(record) : null;
        
        if (matcher != null && !matcher.find()) {
            return null;
        }
        
        return data;
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
