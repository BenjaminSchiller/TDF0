package de.tuda.p2p.tdf.common;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashSet;

import redis.clients.jedis.Jedis;

public class RedisTaskSet extends HashSet<Task> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public RedisTaskSet(Collection<Task> tasks) {
		for (Task task : tasks) add(task);
	}

	public RedisTaskSet() {
		// TODO Auto-generated constructor stub
	}

	public boolean save(Jedis jedis, String RedisKey) {

		for (Task v : this) {
			jedis.sadd(RedisKey, v.getIndex().toString());
		}

		return true;
	}
	
	public boolean load(Jedis jedis, String namespace , String RedisKey){
		this.clear();
		for(String index : jedis.smembers(RedisKey)){
			try{add(new Task(jedis, namespace, Long.valueOf(index)));}catch(FileNotFoundException e){}// TODO: LOG
		}
		return true;
	}

	public TaskLike getany() {
		return this.iterator().next();
	}

}
