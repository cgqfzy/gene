package com.chee.gene.constant;

/**
 * 常量池定义
 * Created by chenguoqing on 17/3/11.
 */
public class Constants {


    //public static final String INPUT_PATH = "hdfs://centos-2:9000/data/gene/input";
    //public static final String INPUT_INIT_CENTERS_PATH = "hdfs://centos-2:9000/data/gene/center";
    //public static final String OUTPUT_PATH = "hdfs://centos-2:9000/data/gene/output/";

    public static final String INPUT_PATH = "/data/gene/input";
    public static final String INPUT_INIT_CENTERS_PATH = "/data/gene/center";
    public static final String OUTPUT_PATH = "/data/gene/output/";


    public static final String PERSON_GENE_DELIMITER = "-";


    public static final String PRE_CENTER_NAME = INPUT_INIT_CENTERS_PATH + "/pre_centers.txt";
    public static final String NEW_CENTER_NAME = INPUT_INIT_CENTERS_PATH + "/new_centers.txt";
    public static final String DATA_FILE_NAME = INPUT_PATH + "/data.txt";



    public static final String PRE_CENTERS = "pre_centers.txt";
    public static final String NEW_CENTERS = "new_centers.txt";


    public static final String BINARY_CLASSFICATION_PATH = "/data/gene/biclf/";
    public static final String JOB_NAME = "Main-";

    public static final int KM_MAX_ITERATE_CNT = 3;

    public static final int CENTER_CNT = 2;
    public static final int GENE_LENGTH = 83;

}
