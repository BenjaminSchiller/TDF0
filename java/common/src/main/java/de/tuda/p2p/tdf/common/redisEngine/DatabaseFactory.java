package de.tuda.p2p.tdf.common.redisEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.InvalidDatabaseKey;
import de.tuda.p2p.tdf.common.NamespaceNotExistant;
import de.tuda.p2p.tdf.common.databaseObjects.LogMessage;
import de.tuda.p2p.tdf.common.databaseObjects.LogMessageQueue;
import de.tuda.p2p.tdf.common.databaseObjects.LogMessageType;
import de.tuda.p2p.tdf.common.databaseObjects.Namespace;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;
import de.tuda.p2p.tdf.common.databaseObjects.TaskListMetainformation;
import redis.clients.jedis.Jedis;

public class DatabaseFactory {
	
	private Jedis jedis;
	private HashMap<String,LogMessageQueue> logQueues;
	
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
		
		logQueues = new HashMap<String,LogMessageQueue>();
	}
	
	public String addSingleTask(Task t) throws NamespaceNotExistant {
		String key = "tdf:" + t.getNamespace() + ":task:" + this.getTaskIndexAndIncrement(t.getNamespace());
		if(!namespaceExists(t.getNamespace()))
			throw new NamespaceNotExistant();
		t.saveToDB(jedis, key);
		jedis.lpush("tdf:" + t.getNamespace() + ":unmergedTasks", key);
		return key;
	}
	
	public DatabaseStringQueue getSingleTasks(String namespace) throws NamespaceNotExistant {
		if(!namespaceExists(namespace))
			throw new NamespaceNotExistant();
		return new DatabaseStringQueue(jedis, "tdf:" + namespace + ":unmergedTasks");
	}
	
	public void saveTask(Task t) {
		t.saveToDB(jedis, t.getDbKey());
	}
	
	public void completedTask(Task t) {
		jedis.sadd("tdf:" + t.getNamespace() + ":completed", t.getDbKey());
	}
	
	public void addSuccessfulTask(Task t) {
		t.saveToDB(jedis, t.getDbKey());
		jedis.lpush("tdf:" + t.getNamespace() + ":successful", t.getDbKey());
		jedis.lpush("tdf:" + t.getNamespace() + ":newlySuccessful", t.getDbKey());
	}
	
	public void addProcessedTask(Task t) {
		t.saveToDB(jedis, t.getDbKey());
		jedis.lpush("tdf:" + t.getNamespace() + ":processed", t.getDbKey());
		jedis.lpush("tdf:" + t.getNamespace() + ":newlyProcessed", t.getDbKey());
	}
	
	public Collection<Task> getProcessedTasks(String namespace) {
		Set<Task> tl = new HashSet<Task>();
		
		for(String dbKey : jedis.lrange("tdf:" + namespace + ":processed", 0, -1)) {
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
		
		jedis.del("tdf:" + namespace + ":newlySuccessful");
		
		return tl;
	}
	
	public Collection<Task> getNewlySuccessfulTasks(String namespace) {
		Set<Task> tl = new HashSet<Task>();
		
		String dbKey = jedis.rpop("tdf:" + namespace + ":newlySuccessful");
		
		while(dbKey != null) { 
			Task t = new Task();
			t.loadFromDB(jedis, dbKey);
			tl.add(t);
			dbKey = jedis.rpop("tdf:" + namespace + ":newlySuccessful");
		}
		
		jedis.del("tdf:" + namespace + ":newlyProcessed");
		
		return tl;
	}
	
	public Collection<Task> getNewlyProcessedTasks() {
		Set<Task> tl = new HashSet<Task>();
		
		for(String namespace : this.getAllNamespaces()) {
			tl.addAll(this.getNewlyProcessedTasks(namespace));
		}
		return tl;
	}
	
	public Collection<Task> getNewlySuccessfulTasks() {
		Set<Task> tl = new HashSet<Task>();
		
		for(String namespace : this.getAllNamespaces()) {
			tl.addAll(this.getNewlySuccessfulTasks(namespace));
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
	 * @throws NamespaceNotExistant 
	 */
	public TaskList generateTaskList(Collection<Task> tasks, String namespace) throws NamespaceNotExistant {
		if(!namespaceExists(namespace))
			throw new NamespaceNotExistant();
		TaskList tl = new TaskList(jedis, "tdf:" + namespace + ":tasklist:" + this.getTaskListIndexAndIncrement(namespace));
		
		for(Task t : tasks) {
			String taskKey = "tdf:" + namespace + ":task:" + this.getTaskIndexAndIncrement(namespace);
			t.saveToDB(jedis, taskKey);
			tl.pushTaskKey(taskKey);
		}
		
		jedis.lpush("tdf:" + namespace + ":queueingTaskLists", tl.getDBKey());
		
		return tl;
	}
	
	public TaskList generateTaskListExistingTasksAndQueue(Collection<String> tasks, String namespace) throws NamespaceNotExistant {
		TaskList tl = generateTaskListExistingTasks(tasks, namespace);
		
		queueTaskList(tl, namespace, true);
		
		return tl;
	}
	
	public TaskList generateTaskListExistingTasks(Collection<String> tasks, String namespace) throws NamespaceNotExistant {
		if(!namespaceExists(namespace))
			throw new NamespaceNotExistant();
		TaskList tl = new TaskList(jedis, "tdf:" + namespace + ":tasklist:" + this.getTaskListIndexAndIncrement(namespace));
		
		for(String t : tasks) {
			tl.pushTaskKey(t);
		}
		
		return tl;
	}
	
	public void queueTaskList(TaskList tl, String namespace, boolean tail) {
		if(tail)
			jedis.lpush("tdf:" + namespace + ":queueingTaskLists", tl.getDBKey());
		else
			jedis.rpush("tdf:" + namespace + ":queueingTaskLists", tl.getDBKey());
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
		jedis.lrem("tdf:" + namespace + ":processed", 0, dbkey);
		jedis.lrem("tdf:" + namespace + ":successful", 0, dbkey);
		jedis.lrem("tdf:" + namespace + ":newlyProcessed", 0, dbkey);
		jedis.lrem("tdf:" + namespace + ":newlySuccessful", 0, dbkey);

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
	
	public Collection<TaskList> getTimedoutTaskLists(String namespace) throws NamespaceNotExistant {
		if(!namespaceExists(namespace))
			throw new NamespaceNotExistant();
		
		LinkedList<TaskList> tlList = new LinkedList<TaskList>();
		
		for(String dbKey : jedis.keys("tdf:" + namespace + ":tasklist:*[0-9]")) {
			TaskList tl = new TaskList(jedis, dbKey);
			if(tl.isOverdue()) {
				tlList.add(tl);
			}
		}
		
		return tlList;
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
	
	public void failTasklist(TaskList tl) {
		for(ClientTask t : tl.getClientTasks()) {
			if(!t.hasField("finished")) {
				addTaskToFailed(t);
			}
		}
		tl.finish();
	}
	
	public void failTasklist(String dbKey) {
		this.failTasklist(new TaskList(jedis, dbKey));
	}
	
	public static JsonNode parseJson(String sJson) throws InvalidSyntaxException {
		JdomParser parser = new JdomParser();
		return parser.parse(sJson.replaceAll(System.lineSeparator(),"").replaceAll("\t",""));
	}
	
	private String extractNamespace(String dbkey) {
		return dbkey.split(":")[1];
	}
	
	public Collection<TaskList> requeue(String namespace, Long listsize, boolean equally) throws NamespaceNotExistant {
		
		ArrayList<String> failedTasks = new ArrayList<String>();
		String taskKey = jedis.lpop("tdf:" + namespace + ":failed");
		
		while(taskKey != null) {
			failedTasks.add(taskKey);
			taskKey = jedis.lpop("tdf:" + namespace + ":failed");
			System.err.println(taskKey);
		}
		
		return generateMultipleTaskListsAndQueue(failedTasks, listsize, equally, false, namespace);
	}
	
	/**
	 * Generate multiple Task lists from a list of Database keys that refer to tasks
	 * Taskslists are added to the Queue to be run
	 * 
	 * @param tasks database keys that refer to tasks
	 * @param tail if true, add tasklists to the tail of the queue, else to the head
	 * @throws NamespaceNotExistant 
	 */
	public Collection<TaskList> generateMultipleTaskListsAndQueue(Collection<String> tasks, Long listsize, boolean equally, boolean tail, String namespace) throws NamespaceNotExistant {
		LinkedList<TaskList> tasklists = new LinkedList<TaskList>();
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
			
			TaskList tl = this.generateTaskListExistingTasks(taskl, namespace);
			this.queueTaskList(tl, namespace, tail);
			tasklists.add(tl);
		}
		
		List<String> taskl = new LinkedList<String>();
		while(numOfTasks !=0)
		{
			String t = iter.next();
			taskl.add(t);
			numOfTasks--;
		}
		
		TaskList tl = this.generateTaskListExistingTasks(taskl, namespace);
		this.queueTaskList(tl, namespace, tail);
		tasklists.add(tl);
		
		return tasklists;
	}
	
	public String showDatabaseEntry(String key) throws InvalidDatabaseKey, NamespaceNotExistant {
		
		String[] dbPath = key.split(":");
		
		StringBuilder output = new StringBuilder();
		
		if(dbPath.length < 3 || !dbPath[0].equals("tdf"))
			throw new InvalidDatabaseKey();
		
		if(!namespaceExists(dbPath[1]))
			throw new NamespaceNotExistant();
		
		switch(dbPath[2]) {
			case "tasklist":
				if(dbPath.length == 5) { //in case meta information has to be shown
					TaskListMetainformation tlm = new TaskListMetainformation();
					tlm.loadFromDB(jedis, key);
					
					output.append(tlm.toJson());
					break;
				}
			case "unmergedTasks":
			case "failed":
			case "processed":
			case "queueingTaskLists":
			case "newlyProcessed":
				DatabaseStringQueue queue = new DatabaseStringQueue(jedis, key);
				boolean first = true;
				for(String item : queue.showQueue()) {
					if(first)
						first = false;
					else
						output.append(System.getProperty("line.separator"));
					output.append(item);
				}
				break;
				
			case "task":
				if(dbPath.length != 4)
					throw new InvalidDatabaseKey();
				Task task = new Task();
				task.loadFromDB(jedis, key);
				
				output.append(task.toJson());
				break;
				
			default:
				throw new InvalidDatabaseKey();
			
		}
		
		return output.toString();
		
	}
	
	/**
	 * Log client side messages
	 * @param client name of the client as given in configuration
	 * @param message log message
	 */
	public void log(String client, LogMessageType type, String param) {
		this.getLogMessageQueue(client).push(new LogMessage(type, param));
	}
	
	private LogMessageQueue getLogMessageQueue(String client) {
		if(logQueues.containsKey(client)) {
			return logQueues.get(client);}
		else {
			LogMessageQueue lmq = new LogMessageQueue(jedis, "tdf:log:" + client);
			logQueues.put(client, lmq);
			return lmq;
		}
	}
	
	public Collection<String> listClientsWithLogs() {
		Set<String> clients = new HashSet<String>();
		
		for(String key : jedis.keys("tdf:log:*")) {
			clients.add(key.split(":")[2]);
		}
		
		return clients;
	}
	

	/**
	 * Get next log message of a specific client
	 * @param client name of the client as given in configuration
	 * @return
	 */
	public String getNextLogMessageBlocking(String client) {
		return jedis.brpop(0, "tdf:log:" + client).get(1);
	}
	
	public String getNextLogMessage(String client) {
		return jedis.rpop("tdf:log:" + client);
	}
	
	/**
	 * Get next log message of multiple specific clients
	 * @param client name of the client as given in configuration
	 * @return
	 */
	/*private String getNextLogMessage(String... clients) {
		return jedis.brpop(0, clients).get(1);
	}*/
	
	/**
	 * Get next log message from any client
	 * @return
	 */
/*	public String getNextLogMessage() {
		return getNextLogMessage(jedis.keys("tdf:log:*").toArray(new String[0]));
	}*/

	public boolean doesTaskStillBelongToClient(String client, Task t) {
		Task tn = new Task();
		
		tn.loadFromDB(jedis, t.getDbKey());
		
		return tn.getField("client").equals(client);
	}
	
	public void killJedisTasks() {
		jedis.disconnect();
		jedis.connect();
	}
	
}
