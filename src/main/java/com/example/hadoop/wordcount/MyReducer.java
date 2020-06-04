package com.example.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
        LongWritable longWritable = new LongWritable();
        AtomicLong sum = new AtomicLong();

        values.forEachRemaining( (value) -> {
            sum.addAndGet(value.get());
        });

        longWritable.set(sum.get());
        outputCollector.collect(key, longWritable);
    }
}
