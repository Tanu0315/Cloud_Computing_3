package com.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TemperatureAggregator aggregator = new TemperatureAggregator();

        for (Text value : values) {
            aggregator.addTemperature(value.toString());
        }

        String result = aggregator.calculateAverageTemperatures();
        context.write(key, new Text(result));
    }

    private static class TemperatureAggregator {
        private double sumMax = 0;
        private double sumMin = 0;
        private int countMax = 0;
        private int countMin = 0;

        public void addTemperature(String temperatureRecord) {
            String[] parts = temperatureRecord.split("_");

            if (parts.length < 2) return;

            double temperature = Double.parseDouble(parts[1]);
            switch (parts[0]) {
                case "TMAX":
                    sumMax += temperature;
                    countMax++;
                    break;
                case "TMIN":
                    sumMin += temperature;
                    countMin++;
                    break;
                default:
                    // Handle unexpected temperature types if necessary
                    break;
            }
        }

        public String calculateAverageTemperatures() {
            double avgMax = countMax > 0 ? sumMax / countMax : 0;
            double avgMin = countMin > 0 ? sumMin / countMin : 0;

            return String.format("%.2f, %.2f", avgMax, avgMin);
        }
    }
}
