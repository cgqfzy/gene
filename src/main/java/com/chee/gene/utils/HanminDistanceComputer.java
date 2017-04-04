package com.chee.gene.utils;

/**
 * Created by chenguoqing on 17/3/11.
 */
public class HanminDistanceComputer {

    /**
     * 计算两个字符串的汉明距离
     *
     * @param src 源字符串
     * @param dst 目标字符串
     * @return 两个字符串的汉明距离, 如果字符串长度不相等, 则返回 -1
     */
    public int getHMDistance(String src, String dst) {
        if (src.length() != dst.length()) {
            return -1;
        }
        int distance = 0;
        for (int i = 0; i < src.length(); i++) {
            if (src.charAt(i) != dst.charAt(i)) {
                distance += 1;
            }
        }
        return distance;
    }

}
