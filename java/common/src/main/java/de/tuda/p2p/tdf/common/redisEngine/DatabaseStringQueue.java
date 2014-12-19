package de.tuda.p2p.tdf.common.redisEngine;

import java.util.List;
import java.util.Collection;
import java.util.LinkedList;

import redis.clients.jedis.Jedis;

public class DatabaseStringQueue {
	private String dbKey;
	protected Jedis jedis;
	
	public DatabaseStringQueue(Jedis jedis, String dbKey) {
		this.dbKey = dbKey;
		this.jedis = jedis;
	}
	
	public void push(String str) {
		jedis.lpush(dbKey, str);
	}
	
	public String pop() {
		return jedis.rpop(dbKey);
	}
	
	public String getDBKey() {
		return dbKey;
	}
	
	public Collection<String> popAllCurrent(){
		List<String> list = new LinkedList<String>();
		String value = pop();
		
		while(value != null) {
			list.add(value);
			value = pop();
		}
		return list;
	}
	
	public List<String> showQueue() {
		return jedis.lrange(dbKey, 0, -1);
	}
	
	public void flush() {
		jedis.del(dbKey);
	}
}
