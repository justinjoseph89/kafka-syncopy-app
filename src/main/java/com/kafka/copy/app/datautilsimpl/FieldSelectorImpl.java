package com.kafka.copy.app.datautilsimpl;

import static com.kafka.copy.app.constants.KafkaConstants.TOPIC_FIELD_DEFAULT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import com.kafka.copy.app.datautils.FieldSelector;

public class FieldSelectorImpl extends FieldSelector {
  private static final Logger logger = Logger.getLogger(FieldSelectorImpl.class.getName());

  /**
   * Default Constructor
   */
  public FieldSelectorImpl() {

  }

  @Override
  public <K, V> long getTimestampFromData(final ConsumerRecord<K, V> rec, final String... fieldNameToConsider) {
    return (fieldNameToConsider[0].equals(TOPIC_FIELD_DEFAULT)) ? rec.timestamp()
        : (rec.value() instanceof GenericRecord) ? findFieldValue((GenericRecord) rec.value(), fieldNameToConsider) : 0;
  }

  @Override
  protected long findFieldValue(GenericRecord genericRecord, String... collectionName) {
    long finalValue = 0L;
    Object collectionRec = genericRecord.get(collectionName[0]);

    if (collectionRec instanceof GenericRecord) {
      final GenericRecord rec = (GenericRecord) collectionRec;
      try {
        return findFieldValue(rec, removeElement(collectionName, collectionName[0]));
      } catch (Exception e) {
        logger.error("No Field with value of Primitive Data Types For Record: " + rec + " " + e.getMessage());
      }

    } else if (collectionRec instanceof GenericArray) {
      @SuppressWarnings("unchecked")
      final GenericArray<GenericRecord> rec = (GenericArray<GenericRecord>) collectionRec;
      try {
        return findFieldValue(rec.get(0), removeElement(collectionName, collectionName[0]));
      } catch (Exception e) {
        logger.error("No Field with value of Primitive Data Types For Record: " + rec + " " + e.getMessage());
      }
    } else {
      finalValue = convertToLong(collectionRec);
    }
    return finalValue;
  }

  @Override
  protected long getMillies(final String date) {

    DateTimeParser[] dateParsers = { DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
        DateTimeFormat.forPattern("dd-MM-yyyy").getParser(),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss").getParser(),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss.SSS").getParser(),
        DateTimeFormat.forPattern("ddMMyyyyHHmmss").getParser() };
    DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter();

    return formatter.parseDateTime(date).getMillis();
  }

  @Override
  protected String[] removeElement(String[] n, String remove) {
    final List<String> list = new ArrayList<String>();
    Collections.addAll(list, n);
    list.remove(remove);
    n = list.toArray(new String[list.size()]);
    return n;
  }

  @Override
  protected long convertToLong(Object collectionRec) {
    long finalValue = 0L;
    if (collectionRec instanceof String) {
      try {
        finalValue = Long.parseLong((String) collectionRec);
      } catch (NumberFormatException e) {
        finalValue = getMillies((String) collectionRec);
      }
    } else if (collectionRec instanceof Long) {
      finalValue = (Long) collectionRec;
    } else if (collectionRec instanceof Utf8) {
      try {
        finalValue = Long.parseLong(collectionRec.toString());
      } catch (NumberFormatException e) {
        finalValue = getMillies(collectionRec.toString());
      }
    } else if (collectionRec instanceof Integer) {
      finalValue = Long.parseLong(collectionRec.toString());
    } else if (collectionRec instanceof Double) {
      finalValue = Long.parseLong(collectionRec.toString());
    }

    return finalValue;
  }

}
