package de.tuda.p2p.tdf.common.redisEngine;

import redis.clients.jedis.Jedis;

public interface DatabaseObject {
	public void loadFromDB(Jedis jedis, String dbKey);
	public void saveToDB(Jedis jedis, String dbKey);
}
