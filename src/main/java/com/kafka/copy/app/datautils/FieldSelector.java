package com.kafka.copy.app.datautils;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class FieldSelector {

  /**
   * This method is to get the timestamp data from the inner fields of the
   * records. This function is designed in such a way that the user knows the
   * exact position of timestamp column they want to use.
   * 
   * @param rec
   * @param fieldNameToConsider : the path for the field should be specified in
   *                            this. Eg: Data,fieldName
   * @return timestamp value of the field given in the configuration.
   */
  abstract public <K, V> long getTimestampFromData(final ConsumerRecord<K, V> rec, final String... fieldNameToConsider);

  /**
   * This private function will find the field value from the position of the
   * field given by the user.
   * 
   * @param genericRecord
   * @param collectionName
   * @return returns the timestamp value of the field provided by user.
   */
  abstract protected long findFieldValue(final GenericRecord genericRecord, final String... collectionName);

  /**
   * This will be used to select the date parser according with the format.
   * 
   * @param date input field value to extract the timestamp
   * @return returns the timestamp value of the field provided by user.
   */
  abstract protected long getMillies(final String date);

  /**
   * This is a common utility function to remove the fields from the string array.
   * Is it okay to place this function in this or should I move it to some common
   * utils ?
   * 
   * @param n      string array where we need to remove the data from
   * @param remove string which needs to be removed from the string array
   * @return a string array after removing the one string
   */
  abstract protected String[] removeElement(final String[] n, final String remove);

  /**
   * This function will convert the selected field to the long value. Is it okay
   * to place this function in this or should I move it to some common utils??
   * 
   * @param collectionRec record to convert to long value
   * @return converted long value
   */
  abstract protected long convertToLong(final Object collectionRec);

}
