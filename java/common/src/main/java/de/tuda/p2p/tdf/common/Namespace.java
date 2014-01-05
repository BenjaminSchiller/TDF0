/**
 * 
 */
package de.tuda.p2p.tdf.common;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author georg
 *
 */
public class Namespace implements TaskLike {
	private Jedis jedis;
	private RedisHash defaults = new RedisHash();

	private String name;

	public Namespace(Jedis jedis, String name) {
		this.setName(name);
		this.setJedis(jedis);
		defaults = new RedisHash();
	}

	private void setJedis(Jedis jedis) {

		this.jedis = jedis;

	}

	/**
	 * 
	 */
	public Namespace() {
		// TODO Auto-generated constructor stub
		defaults = new RedisHash();
	}

	/**
	 * @param worker
	 * @param input
	 * @param runBefore
	 * @param runAfter
	 * @param timeout
	 * @param waitAfterSetupError
	 * @param waitAfterRunError
	 * @param waitAfterSuccess
	 * @param session
	 */

	private String Setkey(){
		return "tdf."+ getName();
	}
	private String HashKey() {
		return "tdf."+ getName() + ".defaults";
	}
	/**
	 * 
	 * @param set
	 *            a Set of tasks to be added
	 * @return the number of Tasks added <=set.size()
	 */
	public Long save(Jedis jedis) {
		
		if(
			jedis==null){
			return -1L;
		}
		// save defaults
		defaults.save(jedis, HashKey());
		
		if (! jedis.exists("tdf."+getName()+".index" ) )jedis.set("tdf."+getName()+".index","0");
		return 0L;

	}
	public Long save(){
		return save(jedis);
	}

	public String asString() {
		// TODO Auto-generated method stub
		return getName();
	}

	public String toString(){
		return asString();
	}
	public String getWorker() {
		return (defaults.get(TaskSetting.Worker)==null?null:defaults.get(TaskSetting.Worker).toString());
	}

	public void setWorker(String worker) {
		defaults.put(TaskSetting.Worker, worker);
	}

	public String getInput() {
		return (defaults.get(TaskSetting.Input)==null?null:defaults.get(TaskSetting.Input).toString());
	}

	public void setInput(String input) {
		defaults.put(TaskSetting.Input, input);
	}
	public String getRunBeforeAsString() {
		DateTime v = getRunBefore();
		return v!=null?v.toString():"";	}

	public DateTime getRunBefore() {
		return (defaults.get(TaskSetting.RunBefore)==null?null:DateTime.parse(defaults.get(TaskSetting.RunBefore).toString()));
	}

	public void setRunBefore(String runBefore) {
		defaults.put(TaskSetting.RunBefore, runBefore);

	}

	public void setRunBefore(DateTime runBefore) {
		setRunBefore(runBefore.toString());
	}

	public String getRunAfterAsString() {
		DateTime v = getRunAfter();
		return v!=null?v.toString():"";
	}

	public DateTime getRunAfter() {
		return (defaults.get(TaskSetting.RunAfter)==null?null:DateTime.parse(defaults.get(TaskSetting.RunAfter).toString()));

	}

	public void setRunAfter(String runAfter) {
		defaults.put(TaskSetting.RunAfter, runAfter);

	}

	public void setRunAfter(DateTime runAfter) {
		setRunAfter(runAfter.toString());
	}
	public Integer getTimeout() {
		return (defaults.get(TaskSetting.Timeout)==null?null:Integer.valueOf(defaults.get(TaskSetting.Timeout).toString()));
	}

	public void setTimeout(Integer timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public void setTimeout(String timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public Integer getWaitAfterSuccess() {
		return (defaults.get(TaskSetting.WaitAfterSuccess)==null?null:Integer.valueOf(defaults.get(TaskSetting.WaitAfterSuccess).toString()));

	}

	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
	}

	public void setWaitAfterSuccess(String waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);

	}

	public Integer getWaitAfterSetupError() {
		return defaults.get(TaskSetting.WaitAfterSetupError)==null?null:Integer.valueOf(defaults.get(TaskSetting.WaitAfterSetupError).toString());
 
	}

	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public void setWaitAfterSetupError(String waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public Integer getWaitAfterRunError() {
		return (defaults.get(TaskSetting.WaitAfterRunError)==null?null:Integer.valueOf(defaults.get(TaskSetting.WaitAfterRunError).toString()));
		 
	}

	public void setWaitAfterRunError(Integer waitAfterRunError) {
		defaults.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public void setWaitAfterRunError(String waitAfterRunError) {
		defaults.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public String getSession() {
		return (defaults.get(TaskSetting.Session)==null?null:defaults.get(TaskSetting.Session).toString());
	}

	public void setSession(String session) {
		defaults.put(TaskSetting.Session, session);
	}

	public boolean isExpired() {
		if (getRunBefore() == null)
			return false;
		return getRunBefore().isBeforeNow();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void applyDefaults(){
		for (TaskList t : getLists()) t.applyDefaults(defaults);
	}

	private Collection<TaskList> getLists() {
		Collection<TaskList> lists = new HashSet<TaskList>();
		for(String id : getJedis().smembers(Setkey())){
			try{lists.add(new TaskList(jedis,name,Long.parseLong(id)));}catch(FileNotFoundException e){} // TODO: LOG
		}
		return lists;
	}

	protected Jedis getJedis() {
		return jedis;
	}

	public long getNewIndex() {
		try {
		return getJedis().incr("tdf." + getName() + ".index");
		}catch(JedisConnectionException e){
			Logger.error("Conection to Jedis timed out, shutting down");
			System.exit(1);
		}
		return 0;		
	}

	public void applyDefaults(RedisHash rh) {
		// set task information
		if (getInput() == null || getInput().isEmpty()) {
			if(rh.get(TaskSetting.Input)!=null) setInput(rh.get(TaskSetting.Input).toString());
		}
		if (getRunBeforeAsString() == null || getRunBeforeAsString().isEmpty()) {
			if(rh.get(TaskSetting.RunBefore)!=null) setRunBefore(rh.get(TaskSetting.RunBefore).toString());
		}
		if (getRunAfterAsString() == null || getRunAfterAsString().isEmpty()) {
			if(rh.get(TaskSetting.RunAfter)!=null) setRunAfter(rh.get(TaskSetting.RunAfter).toString());
		}
		if (getTimeout() == null) {
			if(rh.get(TaskSetting.Timeout)!=null) setTimeout(rh.get(TaskSetting.Timeout).toString());
		}
		if (getWaitAfterSetupError() == null) {
			if(rh.get(TaskSetting.WaitAfterSetupError)!=null) setWaitAfterSetupError(rh.get(TaskSetting.WaitAfterSetupError).toString());
		}
		if (getWaitAfterRunError() == null) {
			if(rh.get(TaskSetting.WaitAfterRunError)!=null) setWaitAfterRunError(rh.get(TaskSetting.WaitAfterRunError).toString());
		}
		if (getWaitAfterSuccess() == null) {
			if(rh.get(TaskSetting.WaitAfterSuccess)!=null) setWaitAfterSuccess(rh.get(TaskSetting.WaitAfterSuccess).toString());
		}

		
	}

	public Collection<Task> getProcessed() {
		Collection<Task> tasks = new LinkedList<Task>();
		for(String id : jedis.smembers("tdf."+getName()+".processed"))
			try{tasks.add(new Task(jedis,name,Long.valueOf(id)));}catch(FileNotFoundException e){} // TODO: LOG
		return tasks;
	}
	public Collection<Task> getRunning() {
		Collection<Task> tasks = new LinkedList<Task>();
		for(String id : jedis.smembers("tdf."+getName()+".running"))
			try{tasks.add(new Task(jedis,name,Long.valueOf(id)));}catch(FileNotFoundException e){} // TODO: LOG
		return tasks;
	}public Collection<Task> getCompleted() {
		Collection<Task> tasks = new LinkedList<Task>();
		for(String id : jedis.smembers("tdf."+getName()+".completed"))
			try {
				tasks.add(new Task(jedis,name,Long.valueOf(id)));
			}catch(FileNotFoundException e){} // TODO: LOG
		return tasks;
	}
	public Collection<Task> getQueued() {
		Collection<Task> tasks = new LinkedList<Task>();
		for(String id : jedis.smembers("tdf."+getName()+".queued"))
			try{tasks.add(new Task(jedis,name,Long.valueOf(id)));}catch(FileNotFoundException e){} // TODO: LOG
		return tasks;
	}
	public Collection<Task> getAllTasks() {
		Collection<Task> tasks = new LinkedList<Task>();
		for(String id : jedis.keys("tdf."+getName()+".task.*"))
			try{tasks.add(new Task(jedis,name,Long.valueOf(id.split("\\.")[3])));}catch(FileNotFoundException e){} // TODO: LOG
		return tasks;
	}
	public Collection<TaskList> getAllTasklists() {
		Collection<TaskList> tasks = new LinkedList<TaskList>();
		for(String id : jedis.keys("tdf."+getName()+".tasklist.*"))
			try {tasks.add(new TaskList(jedis,name,Long.valueOf(id.split("\\.")[3])));}catch(FileNotFoundException e){} // TODO: LOG
		return tasks;
	}
}
