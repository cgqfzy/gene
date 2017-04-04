package com.chee.gene.reduce;

import com.chee.gene.constant.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by chenguoqing on 17/3/15.
 * 计算簇心
 */
public class ScanReducer extends Reducer<IntWritable, IntWritable, Text, NullWritable>{

    private static final Logger logger = Logger.getLogger(ScanReducer.class);
    private List<String> centers;
    private int[] vectors;

    public ScanReducer(){
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        if(context.getCacheFiles() != null && context.getCacheFiles().length > 0){
            String path = context.getCacheFiles()[0].getPath();
            centers = FileUtils.readLines(new File(path));
        }
        if(centers.isEmpty() || centers.size() != Constants.CENTER_CNT){
            logger.error("centors is null or dimendion is not right ,please check!");
        }
        // 初始化
        vectors = new int[centers.size()];
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
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        for(int i = 0; i < vectors.length; i++){
            vectors[i] = 0;
        }
        // 在质心中 则置为1
        for(IntWritable index : values){
            vectors[index.get()] += 1;
        }
        context.write(new Text(key.get() + "-" + intArrayToString()), NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        super.cleanup(context);
    }


    private String intArrayToString(){
        StringBuffer buffer = new StringBuffer();
        for(int i = 0; i < vectors.length; i++){
            buffer.append(vectors[i]);
        }
        return buffer.toString();
    }
}
