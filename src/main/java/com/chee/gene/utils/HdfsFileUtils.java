package com.chee.gene.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenguoqing on 17/3/12.
 */
public class HdfsFileUtils {

    /**
     * @param srcFS
     * @param srcDir
     * @param dstFS
     * @param dstFile
     * @param deleteSource
     * @param conf
     * @param addString
     * @return
     * @throws IOException
     */
    public static boolean copyMerge(FileSystem srcFS,
                                    Path srcDir,
                                    FileSystem dstFS,
                                    Path dstFile,
                                    boolean deleteSource,
                                    Configuration conf,
                                    String addString) throws IOException {
        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
        if (!srcFS.getFileStatus(srcDir).isDirectory()) {
            return false;
        }
        OutputStream out = dstFS.create(dstFile);
        try {
            FileStatus contents[] = srcFS.listStatus(srcDir);
            for (int i = 0; i < contents.length; i++) {
                if (!contents[i].isDirectory()) {
                    InputStream in = srcFS.open(contents[i].getPath());
                    try {
                        IOUtils.copyBytes(in, out, conf, false);
                        if (addString != null) {
                            out.write(addString.getBytes("UTF-8"));
                        }

                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            out.close();
        }


        if (deleteSource) {
            return srcFS.delete(srcDir, true);
        } else {
            return true;
        }
    }

    /**
     * 检查文件是否存在
     *
     * @param srcName
     * @param dstFS
     * @param dst
     * @param overwrite
     * @return
     * @throws IOException
     */
    private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
                                  boolean overwrite) throws IOException {
        if (dstFS.exists(dst)) {
            FileStatus sdst = dstFS.getFileStatus(dst);
            if (sdst.isDirectory()) {
                if (null == srcName) {
                    throw new IOException("Target " + dst + " is a directory");
                }
                return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
            } else if (!overwrite) {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }

    /**
     * 读取文件
     *
     * @param fs
     * @param path
     * @return
     */
    public static List<String> readFileAsStrings(FileSystem fs, Path path) {

        List<String> content = new ArrayList<String>();
        FSDataInputStream fdsis = null;
        BufferedReader br = null;
        try {
            fdsis = fs.open(path);
            br = new BufferedReader(new InputStreamReader(fdsis));
            String line = "";
            while ((line = br.readLine()) != null) {
                content.add(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (fdsis != null) {
                    fdsis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return content;
    }


}
