package com.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

public class JedisClientSingle implements JedisClient{

	private static JedisPool jedisPool;
	static {
		jedisPool = new JedisPool("123.207.137.190",6379);
	}
	@Override
	public String get(String key) {
		Jedis jedis = jedisPool.getResource();
		String string = jedis.get(key);
		jedis.close();
		return string;
	}

	@Override
	public String set(String key, String value) {
		Jedis jedis = jedisPool.getResource();
		String string = jedis.set(key, value);
		jedis.close();
		return string;
	}
	@Override
	public String set(String key, String value,int seconds) {
		Jedis jedis = jedisPool.getResource();
		String string = jedis.setex(key,seconds,value);
		jedis.close();
		return string;
	}
	@Override
	public String hget(String hkey, String key) {
		Jedis jedis = jedisPool.getResource();
		String string = jedis.hget(hkey, key);
		jedis.close();
		return string;
	}

	@Override
	public long hset(String hkey, String key, String value) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.hset(hkey, key, value);
		jedis.close();
		return result;
	}

	@Override
	public long incr(String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.incr(key);
		jedis.close();
		return result;
	}

	@Override
	public long expire(String key, int second) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.expire(key, second);
		jedis.close();
		return result;
	}

	@Override
	public long ttl(String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.ttl(key);
		jedis.close();
		return result;
	}

	@Override
	public long del(String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.del(key);
		jedis.close();
		return result;
	}

	@Override
	public long hdel(String hkey, String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.hdel(hkey, key);
		jedis.close();
		return result;
	}

	@Override
	public Map hgetAll(String userid) {
		Jedis jedis = jedisPool.getResource();
		Map<String,String> map = jedis.hgetAll(userid);
		jedis.close();
		return map;
	}

	@Override
	public long hlen(String hkey) {
		Jedis jedis = jedisPool.getResource();
		Long length = jedis.hlen(hkey);
		jedis.close();
		return length;
	}
	@Override
	public Boolean hexists(String key,String field){
		Jedis jedis = jedisPool.getResource();
		Boolean re = jedis.hexists(key, field);
		jedis.close();
		return re;
	}
	@Override
	public Long zremrangebyrank(String key,long start,long end){
		Jedis jedis = jedisPool.getResource();
		Long res = jedis.zremrangeByRank(key, start, end);
		jedis.close();
		return res;
	}
	@Override
	public Long zadd(String key, Map<String, Double> map){
		Jedis jedis = jedisPool.getResource();
		for (Map.Entry<String,Double> e: map.entrySet()) {
			jedis.zadd(key,e.getValue(),e.getKey());
		}
		jedis.close();
		return Long.valueOf(map.size());
	}
	@Override
	public Long zadd(String key,Double score,String member){
		Jedis jedis = jedisPool.getResource();
		Long nums = jedis.zadd(key,score,member);
		jedis.close();
		return nums;
	}

	@Override
	public Set<Tuple> zrangewithscores(String key, Long start, Long end){
		Jedis jedis = jedisPool.getResource();
		Set<Tuple> set = jedis.zrangeWithScores(key, start, end);
		jedis.close();

		return set;
	}
	@Override
	public Set<Tuple> zrangeByScoreWithScore(String key, String min, String max){
		Jedis jedis = jedisPool.getResource();
		Set<Tuple> set = jedis.zrangeByScoreWithScores(key,min,max);
		jedis.close();
		return set;
	}
	@Override
	public Long zcard(String key){
		Jedis jedis = jedisPool.getResource();
		Long nums = jedis.zcard(key);
		jedis.close();
		return nums;
	}

}
