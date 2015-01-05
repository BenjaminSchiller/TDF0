
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
	
	private TaskListMetainformation config;

	public TaskList(Jedis jedis, String dbKey) {
		super(jedis, dbKey);
		config = new TaskListMetainformation();
		config.loadFromDB(jedis, dbKey + ":meta");
	}
	
	public void start() {
		config.setField("started", new DateTime());
		
		Long totalRuntimeMS = 0L;
		
		for(Task t : this.getTasks()) {
			totalRuntimeMS += (Long) t.getField("timeout");
		}
		
		config.setField("maxRuntime", totalRuntimeMS);
		saveConfig();
	}
	
	public boolean isOverdue() {
		return ((DateTime)config.getField("started")).plusMillis((Integer) config.getField("maxRuntime")).isBeforeNow();
	}
	
	private void saveConfig() {
		config.saveToDB(jedis, config.getDbKey());
	}
	
	public void pushTaskKey(String taskDBkey) {
		this.push(taskDBkey);
	}

	public String popTaskKey() {
		return this.pop();
	}
	
	public Collection<Task> getTasks(){
		
		Vector<Task> tl = new Vector<Task>();
		
		for(String dbKey : this.showQueue()) {
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
		}
		
		return tl;
		/*
		String dbKey = this.popTaskKey();
		
		while(dbKey != null) {
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
			dbKey = this.popTaskKey();
		}
		
		return tl;*/
	}
	
	public Collection<ClientTask> getClientTasks(){
		Vector<ClientTask> tl = new Vector<ClientTask>();
		
		for(String dbKey : this.showQueue()) {
			ClientTask t = new ClientTask();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
		}
		
		return tl;
	}
	
	public void finish() {
		jedis.del(config.getDbKey());
		this.flush();
	}

}
