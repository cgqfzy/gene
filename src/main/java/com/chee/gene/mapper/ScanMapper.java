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
public class ScanMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

    private static final Logger logger = Logger.getLogger(ScanMapper.class);
    private List<String> centors;
    private HanminDistanceComputer hdc;

    private int personID;
    private String gene;

    /**
     * 从分布式缓存文件中读取质心文件
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        if(context.getCacheFiles() != null && context.getCacheFiles().length > 0){
            String path = context.getCacheFiles()[0].getPath();
            centors = FileUtils.readLines(new File(path));
        }
        if(centors.isEmpty() || centors.size() != Constants.CENTER_CNT){
            logger.error("centors is null or dimendion is not right ,please check!");
        }
        hdc = new HanminDistanceComputer();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String[] kg = value.toString().split(Constants.PERSON_GENE_DELIMITER);
        personID = Integer.valueOf(kg[0]);
        gene = kg[1];

        int index = find();
        if(index != -1){
            // personID --> 基因片段在质心的位置
            context.write(new IntWritable(personID), new IntWritable(index));
        }
    }



    /**
     * 查找基因片段在质心中的位置
     * @return
     */
    private int find(){

        for(int i = 0; i < centors.size(); i++){
            if(centors.get(i).equals(gene)){
                return i;
            }
        }
        return -1;
    }

    public ScanMapper(){
    }
}
