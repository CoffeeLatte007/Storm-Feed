package com.util;

import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

/**
 * Created by lz on 2016/6/16.
 */
public interface JedisClient {
    String get(String key);
    String set(String key, String value);

    String set(String key, String value, int seconds);

    String hget(String hkey, String key);
    long hset(String hkey, String key, String value);
    long incr(String key);
    long expire(String key, int second);
    long ttl(String key);
    long del(String key);
    long hdel(String hkey, String key);

    Map hgetAll(String hkey);
    long hlen(String hkey);
    Boolean hexists(String key, String field);
    Long zremrangebyrank(String key, long start, long end);

    Long zadd(String key, Map<String, Double> map);


    Long zadd(String key, Double score, String member);

    Set<Tuple> zrangewithscores(String key, Long start, Long end);

    Set<Tuple> zrangeByScoreWithScore(String key, String min, String max);

    Long zcard(String key);
}
