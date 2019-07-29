package com.lianjia.idmapping;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class IDMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lines = line.split(",");

        MapWritable lj_map = new MapWritable();
        long lj_cnt = 1;

        for(String lj_device_id : lines){
            Text lj_device = new Text(lj_device_id);
            if(lj_map.containsKey(lj_device)){
                LongWritable ss = (LongWritable) lj_map.get(lj_device);
                lj_cnt += ss.get();
            }
            lj_map.put(lj_device,new LongWritable(lj_cnt));
        }

        context.write(new Text(""), lj_map);
    }
}
