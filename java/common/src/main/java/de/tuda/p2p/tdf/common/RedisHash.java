package de.tuda.p2p.tdf.common;

import java.util.HashMap;

import redis.clients.jedis.Jedis;

public class RedisHash extends HashMap<TaskSetting, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9125414486637756342L;
	private Jedis jedis;
	private String RedisKey;

	public RedisHash() {
		// TODO Auto-generated constructor stub
	}
	public RedisHash(Jedis jedis,String RedisKey){
		this.jedis=jedis;
		this.RedisKey=RedisKey;
	}
	
	public boolean save(Jedis jedis,String RedisKey){
		
		for (TaskSetting k : this.keySet()){
			if(get(k) != null) jedis.hset(RedisKey,k.toString(),this.get(k).toString());
		}
		
		
		return true;
	}
	
	public boolean save(){
		return this.save(jedis, RedisKey);
	}
}
