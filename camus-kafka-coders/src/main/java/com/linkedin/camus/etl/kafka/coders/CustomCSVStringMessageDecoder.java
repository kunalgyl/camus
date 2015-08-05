package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Arrays;


/**
 * MessageDecoder class that will convert the payload into a JSON object,
 * look for a the camus.message.timestamp.field, convert that timestamp to
 * a unix epoch long using camus.message.timestamp.format, and then set the CamusWrapper's
 * timestamp property to the record's timestamp.  If the JSON does not have
 * a timestamp or if the timestamp could not be parsed properly, then
 * System.currentTimeMillis() will be used.
 * <p/>
 * camus.message.timestamp.format will be used with SimpleDateFormat.  If your
 * camus.message.timestamp.field is stored in JSON as a unix epoch timestamp,
 * you should set camus.message.timestamp.format to 'unix_seconds' (if your
 * timestamp units are seconds) or 'unix_milliseconds' (if your timestamp units
 * are milliseconds).
 * <p/>
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */

    public class CustomCSVStringMessageDecoder extends MessageDecoder<Message, String> {
    private static final org.apache.log4j.Logger log = Logger.getLogger(CustomCSVStringMessageDecoder.class);
    public static final String CAMUS_MESSAGE_TIMESTAMP_INDEX = "camus.message.timestamp.index";
    public static final String DEFAULT_TIMESTAMP_INDEX = "0";
    public static final String CAMUS_MESSAGE_TIMESTAMP_PARSE = "camus.message.timestamp.parse";
    public static final String DEFAULT_TIMESTAMP_PARSE = "true";


    private String timestampIndex;
    private String timeParse;

  @Override
      public void init(Properties props, String topicName) {
      this.props = props;
      this.topicName = topicName;

      timestampIndex = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_INDEX, DEFAULT_TIMESTAMP_INDEX);
      timeParse = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_PARSE, DEFAULT_TIMESTAMP_PARSE);
  }

  @Override
      public CamusWrapper<String> decode(Message message) {
      long timestamp = 0;
      String payloadString;
      int index;
      Boolean parse;

      try {
	  payloadString = new String(message.getPayload(), "UTF-8"); //Can remove check?
      } catch (UnsupportedEncodingException e) {
	  log.error("Unable to load UTF-8 encoding, falling back to system default", e);
	  payloadString = new String(message.getPayload());
      }

        try {
    parse = Boolean.valueOf(timeParse);
        } catch (Exception e) {
    parse = true;
    log.error("Unable to parse boolean, falling back to default true", e);
      }

      if(!parse) {
          timestamp = System.currentTimeMillis();
          return new CamusWrapper<String>(payloadString, timestamp);
      }

      String[] columns = payloadString.split("\\s*,\\s*");

      try {
	  index = Integer.parseInt(timestampIndex);
	      } catch (NumberFormatException e) {
	  index = 0;
	  log.error("Unable to parse index, falling back to default 0", e);
      }


      try {
	  timestamp = Long.parseLong(columns[index]);
	      } catch (NumberFormatException e) {
	  log.error("Unable to parse Long from CSV of given index", e);
      }

      return new CamusWrapper<String>(payloadString, timestamp);
  }
}