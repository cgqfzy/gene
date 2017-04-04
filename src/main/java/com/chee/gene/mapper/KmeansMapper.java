package com.chee.gene.mapper;

import com.chee.gene.constant.Constants;
import com.chee.gene.utils.HanminDistanceComputer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by chenguoqing on 17/3/11.
 */
public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final Logger logger = Logger.getLogger(KmeansMapper.class);
    private List<String> centors;
    private HanminDistanceComputer hdc;

    /**
     * 从分布式缓存文件中读取质心文件
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            String path = context.getCacheFiles()[0].getPath();
            centors = FileUtils.readLines(new File(path));
        }
        if (centors.isEmpty() || centors.size() != Constants.CENTER_CNT) {
            logger.error("centors is null or dimendion is not right ,please check!");
        }
        hdc = new HanminDistanceComputer();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String gene = value.toString();
        int index = 0;
        int minDistance = Constants.GENE_LENGTH;
        int minDistanceIndex = 0;
        int currentDistance = 0;
        for (String centor : centors) {
            currentDistance = hdc.getHMDistance(centor, gene);
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                minDistanceIndex = index;
            }
            index = 1;
        }
        // 将记录发送到下一个处理节点
        context.write(new IntWritable(minDistanceIndex), new Text(minDistance + "," + gene));
    }

    public KmeansMapper() {
    }
}
