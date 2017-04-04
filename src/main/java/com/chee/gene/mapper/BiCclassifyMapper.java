package com.chee.gene.mapper;

import com.chee.gene.constant.Constants;
import com.chee.gene.utils.HanminDistanceComputer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by chenguoqing on 17/3/14.
 */
public class BiCclassifyMapper extends Mapper<ObjectWritable, Text, Text, NullWritable> {

    private static final Logger logger = Logger.getLogger(BiCclassifyMapper.class);
    private List<String> centers;
    private StringBuffer buffer;
    private HanminDistanceComputer hdc;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            String path = context.getCacheFiles()[0].getPath();
            centers = FileUtils.readLines(new File(path));
        }
        if (centers.isEmpty() || centers.size() != Constants.CENTER_CNT) {
            logger.error("centors is null or dimendion is not right ,please check!");
        }
        buffer = new StringBuffer();
        hdc = new HanminDistanceComputer();

    }

    @Override
    protected void map(ObjectWritable key, Text value, Context context) throws IOException, InterruptedException {
        buffer.setLength(0);
        String gene = value.toString();
        int index = 0;
        int minDistance = Constants.GENE_LENGTH;
        int currentDistance = 0;
        for (int i = 0; i < Constants.CENTER_CNT; i++) {
            currentDistance = hdc.getHMDistance(centers.get(i), gene);
            if (minDistance > currentDistance) {
                minDistance = currentDistance;
                index = i;
            }
        }

        for (int i = 0; i < Constants.CENTER_CNT; i++) {
            if (i == index) {
                buffer.append('1');
            } else {
                buffer.append('0');
            }
        }

        context.write(new Text(buffer.toString()), NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

}
