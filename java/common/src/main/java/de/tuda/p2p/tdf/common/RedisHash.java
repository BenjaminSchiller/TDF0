package de.tuda.p2p.tdf.common;

import java.util.HashMap;

import redis.clients.jedis.Jedis;

public class RedisHash extends HashMap<TaskSetting, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9125414486637756342L;
	protected Jedis jedis;
	protected String RedisKey;

	public RedisHash() {
	}
	public RedisHash(Jedis jedis,String RedisKey){
		this();
		this.jedis=jedis;
		this.RedisKey=RedisKey;
		load(jedis,RedisKey);
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
	
	public boolean load(Jedis jedis,String RedisKey){
		
		for (TaskSetting key : TaskSetting.values()){
			String value= jedis.hget(RedisKey,key.toString());
			if(value != null) this.put(key, value);
		}
		return true;
	}
	
	public boolean equals(Object o){
		
		if ( o.getClass().equals(this.getClass()) &&
				((RedisHash) o).keySet().equals(keySet())				
				) {
			for(TaskSetting k : keySet()){
				if ( ! ((RedisHash) o).get(k).equals(get(k))) return false;
			}
		}else return false;
		
		return true;
	}
}
