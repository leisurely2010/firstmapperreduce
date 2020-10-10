package com.ola.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String [] words=line.split(" ");
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
