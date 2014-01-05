package de.tuda.p2p.tdf.cmd;

import java.io.FileNotFoundException;

import de.tuda.p2p.tdf.common.TaskList;

public class Requeue extends CMD {
	
	public static void main(String[] args){
		init();
		requeue();
	}
	public static Long requeue(){
		
		for (String namespace : jedis.smembers("tdf.namespaces")) requeue(namespace);
		return 0L;
	}
	public static Long requeue(String namespace) {
		Long requeued = 0L;

		// check the running set
		for (String index : jedis.smembers("tdf." + namespace + ".running")) {
			String hashKey = "tdf." + namespace + ".task." + index;

			TaskList task;
			try {
				task = new TaskList(jedis,namespace, Long.valueOf(index));
			} catch ( FileNotFoundException e) {
				break; // TODO Log
			} 

			if (task != null && task.isTimedOut()) {
				requeued++;

				// remove task from the running set
				jedis.srem("tdf." + namespace + ".running", index);

				// remove client and started information
				jedis.hdel(hashKey, "started");
				jedis.hdel(hashKey, "client");

				// add task to the front of the queuing list
				jedis.lpush("tdf." + namespace + ".queuing", index);
			}
		}

		return requeued;
	}

}
