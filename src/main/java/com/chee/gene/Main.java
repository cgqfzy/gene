package com.chee.gene;

import com.chee.gene.constant.Constants;
import com.chee.gene.mapper.BiCclassifyMapper;
import com.chee.gene.mapper.KmeansMapper;
import com.chee.gene.partitioner.KMeansPartitioner;
import com.chee.gene.reduce.KeyPrintlnReducer;
import com.chee.gene.reduce.KmeansReducer;
import com.chee.gene.utils.HanminDistanceComputer;
import com.chee.gene.utils.HdfsFileUtils;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * 主函数
 * Created by chenguoqing on 17/3/11.
 */
public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        Options opt = new Options();
        opt.addOption("i", "input", true, "String: input data path");
        opt.addOption("c", "centers", true, "String: centers path");
        opt.addOption("o", "output", true, "String:output data path");
        opt.addOption("l", "length", true, "int: length of a gene pieces");
        opt.addOption("s", "size", true, "int: number of centers");
        opt.addOption("h", "help", true, "help");

        CommandLineParser parser = new PosixParser();
        CommandLine cl = null;
        String formatstr = "hadoop jar com.chee.gene-1.0.0.jar com.chee.gene.Main -i input data path -c center files -o output data path -l length of gene pieces -s number centers ";
        HelpFormatter hf = new HelpFormatter();
        try {
            cl = parser.parse(opt, args);
        } catch (ParseException e) {
            hf.printHelp(formatstr, "", opt, "");
            System.exit(-1);
        }
        // 如果包含有-h或--help，则打印出帮助信息
        if (cl.hasOption("h")) {
            hf.printHelp(formatstr, "", opt, "");
            System.exit(0);
        }


        Configuration conf = new Configuration();
        if (cl.hasOption("i") && cl.hasOption("c") && cl.hasOption("o") && cl.hasOption("l") && cl.hasOption("s")) {
            try {
                int length = Integer.valueOf(cl.getOptionValue("l"));
                int size = Integer.valueOf(cl.getOptionValue("s"));
                run(conf, cl.getOptionValue("i"), cl.getOptionValue("c"), cl.getOptionValue("o"), Integer.valueOf(cl.getOptionValue("l")), Integer.valueOf(cl.getOptionValue("s")));
            } catch (NumberFormatException e) {
                hf.printHelp(formatstr, "", opt, "please input int value for length and size");
                System.exit(-1);
            }
        } else {
            run(conf);
        }
    }

    /**
     * 启动一个Job
     *
     * @param conf
     * @param input
     * @param center
     * @param output
     * @param length
     * @param size
     * @throws Exception
     */
    public static void run(Configuration conf, String input, String center, String output, int length, int size) throws Exception {
        logger.info("run kmeans job on args: input = " + input + ", center = " + center + ", output = " + output + ", length = " + length + ", size = " + size);
        boolean isDone = false;
        int iteration = 0;
        FileSystem fs = FileSystem.get(conf);
        String lastOutputPath = "";
        // 结束循环
        while (!isDone && iteration < Constants.KM_MAX_ITERATE_CNT) {
            lastOutputPath = output + "/" + System.nanoTime();
            String distributedCachePath = null;

            if (!lunchKMeansIteJob(conf, iteration, input, lastOutputPath, center + "/" + Constants.PRE_CENTERS)) {
                logger.error("run job fail on " + iteration + " iteration!!");
            } else {
                // 将结果文件复制到新文件
                HdfsFileUtils.copyMerge(fs, new Path(lastOutputPath), fs, new Path(center + "/" + Constants.NEW_CENTERS), true, conf, "");
            }

            iteration += 1;

            //比较前后两次质心的距离,如果距离比较小,结束迭代
            logger.info("read pre centers from " + center + "/" + Constants.PRE_CENTERS);
            List<String> preCenters = HdfsFileUtils.readFileAsStrings(fs, new Path(center + "/" + Constants.PRE_CENTERS));
            logger.info("read new centers from " + center + "/" + Constants.NEW_CENTERS);
            List<String> newCenters = HdfsFileUtils.readFileAsStrings(fs, new Path(center + "/" + Constants.NEW_CENTERS));

            Collections.sort(preCenters);
            Collections.sort(newCenters);

            if (preCenters.size() != newCenters.size()) {
                logger.error("centers are illage as pre size = " + preCenters.size() + ", new size = " + newCenters.size());
                System.exit(-1);
            }

            if (getCentersDistance(preCenters, newCenters) <= preCenters.size()) {
                isDone = true;
            }

        }
        // 结束迭代后
        // 将结果文件复制到新文件
        HdfsFileUtils.copyMerge(fs, new Path(lastOutputPath), fs, new Path(center + "/" + Constants.NEW_CENTERS), true, conf, "");
        // 需不需要排序?
        //
        String biClfPath = Constants.BINARY_CLASSFICATION_PATH + System.nanoTime();
        logger.info("output binary classfic result : " + biClfPath);
        int status = lunchBiClassfyJob(conf, lastOutputPath, biClfPath) ? 0 : 1;
        System.exit(status);
    }

    /**
     * 启动一个Job
     *
     * @param conf
     * @throws Exception
     */
    public static void run(Configuration conf) throws Exception {

        boolean isDone = false;
        int iteration = 0;
        FileSystem fs = FileSystem.get(conf);
        String lastOutputPath = "";
        // 结束循环
        while (!isDone && iteration < Constants.KM_MAX_ITERATE_CNT) {
            lastOutputPath = Constants.OUTPUT_PATH + System.nanoTime();
            String distributedCachePath = null;
            if (!lunchKMeansIteJob(conf, iteration, Constants.INPUT_PATH, lastOutputPath, Constants.PRE_CENTER_NAME)) {
                logger.error("run job fail on " + iteration + " iteration!!");
            } else {
                // 将结果文件复制到新文件
                HdfsFileUtils.copyMerge(fs, new Path(lastOutputPath), fs, new Path(Constants.NEW_CENTER_NAME), true, conf, "");
            }

            iteration += 1;

            //比较前后两次质心的距离,如果距离比较小,结束迭代
            List<String> preCenters = HdfsFileUtils.readFileAsStrings(fs, new Path(Constants.PRE_CENTER_NAME));
            List<String> newCenters = HdfsFileUtils.readFileAsStrings(fs, new Path(Constants.NEW_CENTER_NAME));

            Collections.sort(preCenters);
            Collections.sort(newCenters);

            if (preCenters.size() != newCenters.size()) {
                logger.error("centers are illage as pre size = " + preCenters.size() + ", new size = " + newCenters.size());
                System.exit(-1);
            }

            if (getCentersDistance(preCenters, newCenters) <= preCenters.size()) {
                isDone = true;
            }

        }
        // 结束迭代后
        // 将结果文件复制到新文件
        HdfsFileUtils.copyMerge(fs, new Path(lastOutputPath), fs, new Path(Constants.NEW_CENTER_NAME), true, conf, "");
        // 需不需要排序?
        //
        String biClfPath = Constants.BINARY_CLASSFICATION_PATH + System.nanoTime();
        logger.info("output binary classfic result : " + biClfPath);
        int status = lunchBiClassfyJob(conf, lastOutputPath, biClfPath) ? 0 : 1;
        System.exit(status);
    }

    /**
     * 计算前后两个质心点的距离
     *
     * @param preCenters
     * @param newCenters
     * @return
     */
    public static int getCentersDistance(List<String> preCenters, List<String> newCenters) {
        String preGene = "";
        String newGene = "";
        HanminDistanceComputer hdc = new HanminDistanceComputer();
        int dimension = preCenters.size();
        int sumDistance = 0;
        for (int i = 0; i < dimension; i++) {
            sumDistance += hdc.getHMDistance(preCenters.get(i), newCenters.get(i));
        }
        return sumDistance;

    }


    /**
     * 二元分类job
     *
     * @param conf
     * @param centerFilePath
     * @param outputPath
     * @return
     * @throws Exception
     */
    public static boolean lunchBiClassfyJob(Configuration conf, String centerFilePath, String outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Binary Classfication Job");
        job.addCacheFile(new Path(centerFilePath).toUri());
        job.setJarByClass(Main.class);
        job.setMapperClass(BiCclassifyMapper.class);
        job.setReducerClass(KeyPrintlnReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(Constants.DATA_FILE_NAME));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

    /**
     * 启动一个KMeans算法迭代job
     *
     * @param conf
     * @param iteration           循环次数
     * @param input               job输入路径
     * @param output              job输出路径
     * @param distributedFilePath 质心路径
     * @return
     * @throws Exception
     */
    public static boolean lunchKMeansIteJob(Configuration conf, int iteration, String input, String output, String distributedFilePath) throws Exception {
        logger.info("luach job iterations = " + iteration + ",input = " + input + ", output = " + output + ",  centers = " + distributedFilePath);
        Job job = Job.getInstance(conf, Constants.JOB_NAME + iteration);
        job.addCacheFile(new Path(distributedFilePath).toUri());
        job.setJarByClass(Main.class);
        job.setMapperClass(KmeansMapper.class);
        job.setPartitionerClass(KMeansPartitioner.class);
        job.setReducerClass(KmeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }
}
