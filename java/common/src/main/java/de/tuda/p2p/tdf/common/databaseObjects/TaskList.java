
package de.tuda.p2p.tdf.common.databaseObjects;

import java.awt.List;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Set;
import java.util.Vector;

import org.joda.time.DateTime;

import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseHashObject;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseStringQueue;

import redis.clients.jedis.Jedis;

public class TaskList extends DatabaseStringQueue{

	public TaskList(Jedis jedis, String dbKey) {
		super(jedis, dbKey);
	}
	
	public void pushTaskKey(String taskDBkey) {
		this.push(taskDBkey);
	}

	public String popTaskKey() {
		return this.pop();
	}
	
	public Collection<Task> getTasks(){
		Vector<Task> tl = new Vector<Task>();
		
		String dbKey = this.popTaskKey();
		
		while(dbKey != null) {
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
			dbKey = this.popTaskKey();
		}
		
		return tl;
	}
	
	public Collection<ClientTask> getClientTasks(){
		Vector<ClientTask> tl = new Vector<ClientTask>();
		
		String dbKey = this.popTaskKey();
		
		while(dbKey != null) {
			ClientTask t = new ClientTask();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
			dbKey = this.popTaskKey();
		}
		
		return tl;
	}

}
