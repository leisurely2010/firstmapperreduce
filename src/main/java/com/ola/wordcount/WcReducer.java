package com.ola.wordcount;

import java.io.IOException;
public class WcReducer extends org.apache.hadoop.mapreduce.Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (Object value : values) {
            sum+=Integer.parseInt(value.toString());
        }
        context.write(key,sum);
    }
}
