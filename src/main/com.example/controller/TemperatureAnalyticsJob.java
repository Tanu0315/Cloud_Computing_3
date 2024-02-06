package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.TemperatureMapper;
import com.example.TemperatureReducer;
import com.example.TemperatureCombiner;

public class TemperatureAnalyticsJob {

    public static void main(String[] args) throws Exception {
        // Validate input arguments
        if (!areArgumentsValid(args)) {
            System.err.println("Invalid arguments. Usage: TemperatureAnalyticsJob <month> <input path> <output path> [useCombiner]");
            System.exit(2);
        }

        // Configure and run the Hadoop job
        boolean jobCompletedSuccessfully = configureAndRunJob(args);
        System.exit(jobCompletedSuccessfully ? 0 : 1);
    }

    private static boolean areArgumentsValid(String[] args) {
        return args.length >= 3;
    }

    private static boolean configureAndRunJob(String[] args) throws Exception {
        // Extract arguments
        String month = args[0];
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        boolean useCombiner = args.length > 3 && "true".equalsIgnoreCase(args[3]);

        // Set up the job configuration
        Configuration conf = new Configuration();
        conf.set("month", month);

        Job job = Job.getInstance(conf, "Monthly Average Temperature Calculation");
        job.setJarByClass(TemperatureAnalyticsJob.class);
        setupJob(job, useCombiner, inputPath, outputPath);

        // Execute the job
        return job.waitForCompletion(true);
    }

    private static void setupJob(Job job, boolean useCombiner, Path inputPath, Path outputPath) throws Exception {
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        if (useCombiner) {
            job.setCombinerClass(TemperatureCombiner.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }
}
