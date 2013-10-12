package de.tuda.p2p.tdf.common;

import redis.clients.jedis.Jedis;

public class Database {

	private Jedis jedis;

	public Database(String host) {
		jedis=new Jedis(host);
		// TODO Auto-generated constructor stub
	}



}
