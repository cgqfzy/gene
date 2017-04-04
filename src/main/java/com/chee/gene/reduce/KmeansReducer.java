package com.chee.gene.reduce;

import com.chee.gene.constant.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by chenguoqing on 17/3/11.
 * 计算簇心
 */
public class KmeansReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    private static final Logger logger = Logger.getLogger(KmeansReducer.class);

    public KmeansReducer() {
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    /**
     * 计算簇心
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        long sum = 0;
        for (Text value : values) {
            sum += Integer.valueOf(value.toString().split(",")[0]);
            count += 1;
        }
        // 距离平均值
        double avg = sum / count;
        logger.info("avg distance of " + key.toString() + " aggs is : " + avg);
        String minDistanceGene = "";
        double curDistanceToAvg = 0.0D;
        double minDistanceToAvg = Constants.GENE_LENGTH;
        for (Text value : values) {
            String[] dg = value.toString().split(",");
            String gene = dg[1];
            int curDistance = Integer.valueOf(dg[0]);
            // 当前点到平均值的距离
            curDistanceToAvg = Math.abs(curDistance - avg);
            if (curDistanceToAvg < minDistanceToAvg) {
                minDistanceToAvg = curDistanceToAvg;
                minDistanceGene = gene;
            }
        }
        logger.info("the nearest of " + key.toString() + " aggs is : " + minDistanceGene);
        context.write(new Text(minDistanceGene), NullWritable.get());

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }


}
