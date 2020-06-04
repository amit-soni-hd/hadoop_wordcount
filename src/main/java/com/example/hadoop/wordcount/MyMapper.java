package com.example.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private Text text = new Text();

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        StringTokenizer words = new StringTokenizer(value.toString());

        while (words.hasMoreTokens()) {
            text.set(words.nextToken());
            outputCollector.collect(text, one);
        }
    }
}
