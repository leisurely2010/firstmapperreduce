package com.ola.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws Exception {
        //1、获取job
        Job job=Job.getInstance(new Configuration());
        //2、设置jar类
        job.setJarByClass(WcDriver.class);
        //3、设置mapper和reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);
        //3、设置map、reduce输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //4、设置输入参数
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //5、提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
