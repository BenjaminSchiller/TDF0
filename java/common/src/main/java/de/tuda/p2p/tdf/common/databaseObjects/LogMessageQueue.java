package de.tuda.p2p.tdf.common.databaseObjects;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import redis.clients.jedis.Jedis;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseStringQueue;

public class LogMessageQueue extends DatabaseStringQueue {

	public LogMessageQueue(Jedis jedis, String dbKey) {
		super(jedis, dbKey);
	}
	
	public void push(LogMessage lm) {
		this.push(lm.toString());
	}
	
	//FIXME: Needed?
	public LogMessage logPop() {
		return new LogMessage(this.pop());
	}
	
	//FIXME: Needed?
	public Collection<LogMessage> logPopAllCurrent() {
		List<LogMessage> list = new LinkedList<LogMessage>();
		LogMessage value = logPop();
		
		while(value != null) {
			list.add(value);
			value = logPop();
		}
		return list;
	}

}
