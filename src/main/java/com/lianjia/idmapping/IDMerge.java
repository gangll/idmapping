package com.lianjia.idmapping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IDMerge {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(IDMerge.class);

        job.setMapperClass(IDMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/user/hive/warehouse/tmp.db/tmp_dw_log_app_device_da_testx20190728000000"));

        job.setReducerClass(IDReducer.class);
        job.setOutputKeyClass(MapWritable.class);
        job.setOutputValueClass(MapWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/tmp.db/tmp_dw_log_app_device_da_testx20190728000000/out"));

        job.waitForCompletion(true);
    }

}
