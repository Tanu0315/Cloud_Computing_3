package com.example;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Refactored version of the TemperatureMapper class to improve code readability and efficiency.
 * This class maps input data to key-value pairs based on month and temperature type.
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int DATE_PART = 1;
    private static final int TYPE_PART = 2;
    private static final int VALUE_PART = 3;

    /**
     * Maps input records to output key-value pairs, filtering by the specified month and temperature type.
     * 
     * @param key The input key.
     * @param value The input text value, expected to be comma-separated.
     * @param context The mapper context.
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the operation is interrupted.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");

        // Ensure the record has enough parts to avoid IndexOutOfBoundsException
        if (parts.length < 4) return;

        String targetMonth = context.getConfiguration().get("month");
        String date = parts[DATE_PART];
        String type = parts[TYPE_PART];
        String tempValue = parts[VALUE_PART];

        if (isValidTemperatureRecord(date, type, targetMonth)) {
            context.write(new Text(date), new Text(type + "_" + tempValue));
        }
    }

    /**
     * Validates if the record's date matches the target month and the type is either TMAX or TMIN.
     * 
     * @param date The date part of the record.
     * @param type The type part of the record (TMAX or TMIN).
     * @param targetMonth The month to filter the records by.
     * @return True if the record is valid and matches the criteria, false otherwise.
     */
    private boolean isValidTemperatureRecord(String date, String type, String targetMonth) {
        return date.startsWith(targetMonth) && ("TMAX".equals(type) || "TMIN".equals(type));
    }
}
