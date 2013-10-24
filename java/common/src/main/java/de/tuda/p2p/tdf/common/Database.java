package de.tuda.p2p.tdf.common;

import redis.clients.jedis.Jedis;

public class Database {

	private Jedis jedis;

	public Database(String host) {
		jedis=new Jedis(host);
		
	}
	public Database(String host, int port) {
		jedis=new Jedis(host,port);
		
	}
	public Jedis getJedis() {
		return jedis;
	}
	public void setJedis(Jedis jedis) {
		this.jedis = jedis;
	}
	
	



}
