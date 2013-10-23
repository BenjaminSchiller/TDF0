package de.tuda.p2p.tdf.common;

import java.util.HashSet;

import redis.clients.jedis.Jedis;

public class RedisTaskSet extends HashSet<Task> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public boolean save(Jedis jedis, String RedisKey) {

		for (Task v : this) {
			jedis.sadd(RedisKey, v.getIndex().toString());
		}

		return true;
	}
	
	public boolean load(Jedis jedis, String namespace , String RedisKey){
		this.clear();
		for(String index : jedis.smembers(RedisKey)){
			add(new Task(jedis, namespace, Long.valueOf(index)));
		}
		return true;
	}

}
