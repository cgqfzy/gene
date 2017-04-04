package com.chee.gene.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by chenguoqing on 17/3/11.
 */
public class Utils {

    private final Logger Log = Logger.getLogger(Utils.class);

    public static List<String> readCentors(String fileName) {
        try {
            return FileUtils.readLines(new File(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
