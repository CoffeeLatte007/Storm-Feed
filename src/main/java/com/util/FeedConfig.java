package com.util;

/**
 * Created by Administrator on 2016/8/1.
 */
public interface FeedConfig {
    public static String FEED_TOPIC= "CalFedd";
    public static final String FeedIdentifier = "0";
    public static String followers = ":followers";
    public static String following = ":following";
    public static String dynamic = ":dynamic";
    public static String feed  = ":feed";
    public static String imageInfo = ":imageInfo";
    public static Integer MAX_REDIS = 3;
}
