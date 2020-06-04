package com.example.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        String otherArgs[] = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.out.println("Please enter the file input and output path");
            System.exit(2);
        }
        JobConf job = new JobConf(configuration, WordCount.class);

        job.setJobName("word-count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length-2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        boolean result = JobClient.runJob(job).isComplete();

        if (result)
            System.out.println("Job successfully done");
        else
            System.out.println("Something issue with job to be done");
    }
}
