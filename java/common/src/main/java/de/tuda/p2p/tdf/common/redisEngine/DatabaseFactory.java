package de.tuda.p2p.tdf.common.redisEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import argo.jdom.JdomParser;
import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;

import de.tuda.p2p.tdf.common.databaseObjects.Namespace;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;
import redis.clients.jedis.Jedis;

public class DatabaseFactory {
	
	private Jedis jedis;
	
	public DatabaseFactory(String hostname, String port, String index, String password) {
		if(hostname == null || hostname.isEmpty())
			hostname = "localhost";
		
		if(port == null || port.isEmpty())
			port = "6379";
		
		if(index == null || index.isEmpty())
			index = "0";
		
		jedis = new Jedis(hostname, Integer.valueOf(port));
		
		jedis.select(Integer.valueOf(index));
		
		if(password != null && !password.isEmpty())
			jedis.auth(password);
	}
	
	public String addSingleTask(Task t) {
		String key = "tdf:" + t.getNamespace() + ":task:" + this.getTaskIndexAndIncrement(t.getNamespace());
		t.saveToDB(jedis, key);
		jedis.lpush("tdf:" + t.getNamespace() + ":unmergedTasks", key);
		return key;
	}
	
	public DatabaseStringQueue getSingleTasks(String namespace) {
		return new DatabaseStringQueue(jedis, "tdf:" + namespace + ":unmergedTasks");
	}
	
	public void saveTask(Task t) {
		t.saveToDB(jedis, t.getDbKey());
	}
	
	public void completedTask(Task t) {
		jedis.sadd("tdf:" + t.getNamespace() + ":completed", t.getDbKey());
	}
	
	public void addProcessedTask(Task t) {
		t.saveToDB(jedis, t.getDbKey());
		jedis.sadd("tdf:" + t.getNamespace() + ":processed", t.getDbKey());
		jedis.lpush("tdf:" + t.getNamespace() + ":newlyProcessed", t.getDbKey());
	}
	
	public Collection<Task> getProcessedTasks(String namespace) {
		Set<Task> tl = new HashSet<Task>();
		
		for(String dbKey : jedis.smembers("tdf:" + namespace + ":processed")) {
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
		}
		
		return tl;
	}
	
	public Collection<Task> getProcessedTasks() {
		Set<Task> tl = new HashSet<Task>();
		
		for(String namespace : this.getAllNamespaces()) {
			tl.addAll(this.getProcessedTasks(namespace));
		}
		return tl;
	}
	
	public Collection<Task> getNewlyProcessedTasks(String namespace) {
		Set<Task> tl = new HashSet<Task>();
		
		String dbKey = jedis.rpop("tdf:" + namespace + ":newlyProcessed");
		
		while(dbKey != null) { 
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
			dbKey = jedis.rpop("tdf:" + namespace + ":newlyProcessed");
		}
		
		return tl;
	}
	
	public Collection<Task> getNewlyProcessedTasks() {
		Set<Task> tl = new HashSet<Task>();
		
		for(String namespace : this.getAllNamespaces()) {
			tl.addAll(this.getNewlyProcessedTasks(namespace));
		}
		return tl;
	}
	
	public Collection<Task> getFailedTasks(String namespace) {
		Set<Task> tl = new HashSet<Task>();
		
		for(String dbKey : jedis.lrange("tdf:" + namespace + ":failed", 0, -1)) { 
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
		}
		
		return tl;
	}
	
	public Collection<Task> getFailedTasks() {
		Set<Task> tl = new HashSet<Task>();
		
		for(String namespace : this.getAllNamespaces()) {
			tl.addAll(this.getFailedTasks(namespace));
		}
		return tl;
	}
	
	public void mergeSingleTasksToList(Long listsize) {
		
	}
	
	/**
	 * TaskList is saved automatically at the moment
	 */
	public TaskList generateTaskList(Collection<Task> tasks, String namespace) {
		TaskList tl = new TaskList(jedis, "tdf:" + namespace + ":tasklist:" + this.getTaskListIndexAndIncrement(namespace));
		
		for(Task t : tasks) {
			String taskKey = "tdf:" + namespace + ":task:" + this.getTaskIndexAndIncrement(namespace);
			t.saveToDB(jedis, taskKey);
			tl.pushTaskKey(taskKey);
		}
		
		jedis.lpush("tdf:" + namespace + ":queueingTaskLists", tl.getDBKey());
		
		return tl;
	}
	
	public TaskList generateTaskListExisting(Collection<String> tasks, String namespace) {
		TaskList tl = new TaskList(jedis, "tdf:" + namespace + ":tasklist:" + this.getTaskListIndexAndIncrement(namespace));
		
		for(String t : tasks) {
			tl.pushTaskKey(t);
		}
		
		jedis.lpush("tdf:" + namespace + ":queueingTaskLists", tl.getDBKey());
		
		return tl;
	}
	
	public void addNamespace(String n) {
		jedis.sadd("tdf:namespaces", n);
	}
	
	public boolean namespaceExists(String n) {
		return jedis.sismember("tdf:namespaces", n);
	}
	
	public void deleteNamespace(String n) {
		for(String key : jedis.keys("tdf:" + n + ":*")) {
			jedis.del(key);
		}
		jedis.srem("tdf:namespaces", n);
	}
	
	public boolean deleteTask(String dbkey) {
		String namespace = this.extractNamespace(dbkey);
		
		for(String tasklKey : jedis.keys("tdf:" + namespace + ":tasklist:*")) {
			jedis.lrem(tasklKey, 0, dbkey);
		}
		
		jedis.lrem("tdf:" + namespace + ":unmergedTasks", 0, dbkey);
		jedis.lrem("tdf:" + namespace + ":failed", 0, dbkey);
		jedis.srem("tdf:" + namespace + ":processed", dbkey);
		jedis.lrem("tdf:" + namespace + ":newlyProcessed", 0, dbkey);
		
		if(jedis.del(dbkey) == 1)
			return true;
		else
			return false;
	}
	
	public boolean deleteTaskList(String dbkey) {
		String namespace = this.extractNamespace(dbkey);

		jedis.lrem("tdf:" + namespace + ":queueingTaskLists", 0, dbkey);
		
		if(jedis.del(dbkey) == 1)
			return true;
		else
			return false;
	}
	
	public Set<String> getAllNamespaces() {
		return jedis.smembers("tdf:namespaces");
	}
	
	private String getRandomString(Collection<String> collection) {
		return (String) collection.toArray()[new Random().nextInt(collection.size())];
	}

	public TaskList getOpenTaskList(String namespace) {
		return new TaskList(jedis, jedis.brpop(0, "tdf:" + namespace + ":queueingTaskLists").get(1));
	}
	
	public TaskList getOpenTaskList(Collection<String> namespaces) {
		return getOpenTaskList(this.getRandomString(namespaces));
	}
	
	public TaskList getOpenTaskList() {
		Set<String> namespaces = this.getAllNamespaces();
		List<String> queues = new ArrayList<String>();
		
		for(String namespace : namespaces) {
			queues.add("tdf:" + namespace + ":queueingTaskLists");
		}
		
		Collections.shuffle(queues);
				
		return new TaskList(jedis, jedis.brpop(0, queues.toArray(new String[0])).get(1));
	}
	
	private Long getTaskIndexAndIncrement(String n) {
		// Jedis incr function sets value to 1 if nonexistant
		return jedis.incr("tdf:" + n + ":taskIndex");
	}
	
	private Long getTaskListIndexAndIncrement(String n) {
		return jedis.incr("tdf:" + n + ":taskListIndex");
	}
	
	public void addTaskToFailed(Task t) {
		jedis.lpush("tdf:" + t.getNamespace() + ":failed", t.getDbKey());
	}
	
	public Collection<String> requeueOrphanTasks() {
		return null;
	}
	
	public Collection<String> requeueFailedTasks() {
		return null;	
	}
	
	public static JsonNode parseJson(String sJson) throws InvalidSyntaxException {
		JdomParser parser = new JdomParser();
		return parser.parse(sJson.replaceAll(System.lineSeparator(),"").replaceAll("\t",""));
	}
	
	private String extractNamespace(String dbkey) {
		return dbkey.split(":")[1];
	}
	
	public Collection<String> requeue() {
		ArrayList<String> requeued = new ArrayList<String>();
		Set<String> namespaces = this.getAllNamespaces();
		
		for(String namespace : namespaces) {
			requeued.addAll(this.requeue(namespace));
		}
		
		return requeued;
		
	}
	
	public Collection<String> requeue(String namespace) {
		String taskKey = jedis.lpop("tdf:" + namespace + ":failed");
		ArrayList<String> requeued = new ArrayList<String>();
		
		while(taskKey != null) {
			TaskList tl = new TaskList(jedis, "tdf:" + namespace + ":tasklist:" + this.getTaskListIndexAndIncrement(namespace));
			tl.pushTaskKey(taskKey);
			jedis.rpush("tdf:" + namespace + ":queueingTaskLists", tl.getDBKey());
			requeued.add(taskKey);
			taskKey = jedis.lpop("tdf:" + namespace + ":failed");
		}
		return requeued;
	}
	
	/**
	 * Generate multiple Task lists from a list of Database keys that refer to tasks
	 * Taskslists are added to the Queue to be run
	 * 
	 * @param tasks database keys that refer to tasks
	 */
	public TaskList generateMultipleTaskLists(Collection<String> tasks, Long listsize, boolean equally, String namespace) {
		TaskList tasklist = null;
		Integer numOfTasks = tasks.size();
		
		Long finalListsize = 0L;
		
		if(equally)
			finalListsize = (long) Math.ceil(numOfTasks / listsize);
		else
			finalListsize = listsize;
		
		Iterator<String> iter = tasks.iterator();
		
		while(numOfTasks > finalListsize) {
			List<String> taskl = new LinkedList<String>();
			for(int i = 0; i < finalListsize; i++) {
				String t = iter.next();
				taskl.add(t);
				numOfTasks--;
			}
			tasklist = this.generateTaskListExisting(taskl, namespace);
		}
		
		List<String> taskl = new LinkedList<String>();
		while(numOfTasks !=0)
		{
			String t = iter.next();
			taskl.add(t);
			numOfTasks--;
		}
		tasklist = this.generateTaskListExisting(taskl, namespace);

		return tasklist;
	}
}
