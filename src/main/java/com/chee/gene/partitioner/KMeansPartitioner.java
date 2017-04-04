package com.chee.gene.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * partitioner
 * Created by chenguoqing on 17/3/12.
 */
public class KMeansPartitioner extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable intWritable, Text text, int numPartitions) {
        return intWritable.get();
    }
}
