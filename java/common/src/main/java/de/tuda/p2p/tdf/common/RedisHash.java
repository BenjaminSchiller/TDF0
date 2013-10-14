package de.tuda.p2p.tdf.common;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisHash extends HashMap<TaskSetting, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9125414486637756342L;

	public RedisHash() {
		// TODO Auto-generated constructor stub
	}

	public RedisHash(int initialCapacity) {
		super(initialCapacity);
		// TODO Auto-generated constructor stub
	}

	public RedisHash(Map<? extends TaskSetting, ? extends Object> m) {
		super(m);
		// TODO Auto-generated constructor stub
	}

	public RedisHash(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
		// TODO Auto-generated constructor stub
	}
	
	public boolean save(Jedis jedis,String RedisKey){
		
		for (TaskSetting k : this.keySet()){
			jedis.hset(RedisKey,k.toString(),this.get(k).toString());
		}
		
		
		return true;
	}
}
