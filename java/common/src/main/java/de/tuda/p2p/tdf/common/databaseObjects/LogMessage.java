package de.tuda.p2p.tdf.common.databaseObjects;

import redis.clients.jedis.Jedis;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseObject;


public class LogMessage {
	
	Long timestamp;
	LogMessageType type;
	String parameter;	

	public LogMessage(LogMessageType type, String parameter, Long timestamp) {
		this.timestamp = timestamp;
		this.type = type;
		this.parameter = parameter;
	}

	public LogMessage(LogMessageType type, String parameter) {
		this(type, parameter, System.currentTimeMillis() / 1000L);
	}
	
	public LogMessage(String stringReprentation) {
		int i = 0;
		for(String string : stringReprentation.split(":")) {
			if(i == 0) {
				this.timestamp = Long.parseLong(string);
			}
			else if (i == 1) {
				this.type = LogMessageType.fromString(string);
			}
			else if (i == 2) {
				this.parameter = string;
			}
		}
	}
	
	public String toString() {
		if(parameter.isEmpty())
			return timestamp.toString() + ":" + type.getText();
		else
			return timestamp.toString() + ":" + type.getText() + ":" + parameter;
	}
	
}
