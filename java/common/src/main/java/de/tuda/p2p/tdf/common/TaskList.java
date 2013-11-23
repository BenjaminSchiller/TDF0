/**
 * 
 */
package de.tuda.p2p.tdf.common;

import java.util.Set;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;

/**
 * @author georg
 * 
 */
public class TaskList implements TaskLike {

	private RedisTaskSet tasks = new RedisTaskSet();
	private String namespace;
	private Jedis jedis;
	private RedisHash defaults = new RedisHash();
	private String client;
	private DateTime started;
	private DateTime finished;
	private Long index;

	/**
	 * @param jedis
	 * @param namespace
	 * @param index
	 */
	public TaskList(Jedis jedis, String namespace) {
		this.setNamespace(namespace);
		this.setJedis(jedis);

	}
	public TaskList(Jedis jedis, String namespace,Long index) {
		this(jedis,namespace);
		setIndex(index);
		load(jedis,namespace,index);
	}

	private void setJedis(Jedis jedis) {

		this.jedis = jedis;

	}

	/**
	 * 
	 */
	public TaskList() {
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

	private String ListKey(String namespace, Long index) {
		return "tdf." + namespace + ".tasklist." + index;
	}

	private String HashKey(String namespace, Long index) {
		return ListKey(namespace, index) + ".defaults";
	}

	private String SetKey(String namespace, Long index) {
		return ListKey(namespace, index) + ".tasks";
	}

	/**
	 * 
	 * @param task
	 *            the Task to be added
	 * @return the number of Tasks added (0 or 1)
	 */
	public long addtask(Task task) {
		if (tasks.add(task))
			return 1L;

		return 0L;
	}

	/**
	 * 
	 * @param set
	 *            a Set of tasks to be added
	 * @return the number of Tasks added <=set.size()
	 */
	public long addtask(Set<Task> set) {
		int counter = 0;

		for (Task t : set) {
			counter += addtask(t);
		}

		return counter;
	}
	
	public TaskList load(Jedis jedis, String namespace, Long index) {
		
		if(
			jedis==null||
			namespace==null||
			namespace==""||
			index==null){
			return null;
		}
		
		tasks.load(jedis,namespace,SetKey(namespace, index));
		defaults.load(jedis, HashKey(namespace, index));
		// load defaults
		return this;

	}

	public Long save(Jedis jedis, String namespace, Long index) {
		
		if(
			jedis==null||
			namespace==null||
			namespace==""||
			index==null){
			return -1L;
		}
		tasks.save(jedis,SetKey(namespace, index));
		defaults.save(jedis, HashKey(namespace, index));
		// save defaults
		return 0L;

	}
	public Long save(){
		return save(jedis,getNamespace(),getIndex());
	}

	public String asString() {
		// TODO Auto-generated method stub
		return this.toString();
	}

	public Long save(Jedis jedis) {
		return this.save(jedis, this.getNamespace());

	}

	public Long save(Jedis jedis, String namespace) {
		return this.save(jedis, namespace, this.getIndex());
	}

	public String getWorker() {
		return defaults.get(TaskSetting.Worker).toString();
	}

	public void setWorker(String worker) {
		defaults.put(TaskSetting.Worker, worker);
	}

	public String getInput() {
		return defaults.get(TaskSetting.Input).toString();
	}

	public void setInput(String input) {
		defaults.put(TaskSetting.Input, input);
	}

	public String getClient() {
		return client;
	}

	void setClient(String client) {

		this.client = client;
	}

	public String getStartedAsString() {
		return getStarted().toString();
	}

	public DateTime getStarted() {
		return started;
	}

	void setStarted(DateTime started) {
		this.started = started;

	}

	void setStarted(String started) {
		this.started = DateTime.parse(started);
	}

	public String getFinishedAsString() {
		return getFinished().toString();
	}

	public DateTime getFinished() {
		return finished;
	}

	void setFinished(DateTime finished) {
		this.finished = finished;
	}

	void setFinished(String finished) {
		this.finished = DateTime.parse(finished);
	}

	public String getRunBeforeAsString() {
		return getRunBefore().toString();
	}

	public DateTime getRunBefore() {
		return DateTime.parse(defaults.get(TaskSetting.RunBefore).toString());
	}

	public void setRunBefore(String runBefore) {
		defaults.put(TaskSetting.RunBefore, runBefore);

	}

	public void setRunBefore(DateTime runBefore) {
		setRunBefore(runBefore.toString());
	}

	public String getRunAfterAsString() {
		return this.getRunAfter().toString();
	}

	public DateTime getRunAfter() {
		return DateTime.parse(defaults.get(TaskSetting.RunAfter).toString());

	}

	public void setRunAfter(String runAfter) {
		defaults.put(TaskSetting.RunAfter, runAfter);

	}

	public void setRunAfter(DateTime runAfter) {
		setRunAfter(runAfter.toString());
	}

	public Integer getTimeout() {
		return Integer.valueOf(defaults.get(TaskSetting.Timeout).toString());
	}

	public void setTimeout(Integer timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public void setTimeout(String timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public Integer getWaitAfterSuccess() {
		return Integer.valueOf(defaults.get(TaskSetting.WaitAfterSuccess).toString());

	}

	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
	}

	public void setWaitAfterSuccess(String waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);

	}

	public Integer getWaitAfterSetupError() {
		return Integer.valueOf(defaults.get(TaskSetting.WaitAfterSetupError).toString());
 
	}

	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public void setWaitAfterSetupError(String waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public Integer getWaitAfterRunError() {
		return Integer.valueOf(defaults.get(TaskSetting.WaitAfterRunError).toString());
		 
	}

	public void setWaitAfterRunError(Integer waitAfterRunError) {
		defaults.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public void setWaitAfterRunError(String waitAfterRunError) {
		defaults.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public Long getIndex() {
		return index;
	}

	public void setIndex(Long index) {
		this.index = index;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getSession() {
		return defaults.get(TaskSetting.Session).toString();
	}

	public void setSession(String session) {
		defaults.put(TaskSetting.Session, session);
	}

	public boolean isExpired() {
		if (getRunBefore() == null)
			return false;
		return getRunBefore().isBeforeNow();
	}

	public boolean isValid() {
		if (getRunAfter() == null)
			return true;
		return getRunAfter().isBeforeNow();
	}

	public Long validWaitTime() {
		Long waitTime = getRunAfter().getMillis() - DateTime.now().getMillis();
		return (waitTime > 0) ? waitTime : 0;
	}

	public boolean isTimedOut() {
		if (started == null || getTimeout() == null)
			return false;

		if (getRunAfter() == null)
			return started.plusMillis(getTimeout()).isBeforeNow();

		return getRunAfter().plusMillis(getTimeout()).isBeforeNow();

	}

	public boolean isStarted() {
		return (started != null);
	}

	public boolean isFinished() {
		return (getFinished()!=null);
	}

	public void start(String client) {
		setClient(client);
		setStarted(DateTime.now());
	}

	public RedisTaskSet getTasks() {
		// TODO Auto-generated method stub
		return tasks;
	}
	
	public RedisTaskSet getOpenTasks() {
		RedisTaskSet tasks = new RedisTaskSet(getTasks());
		for (Task task : tasks){
			if (task.isFinished()) tasks.remove(task);
		}
		return tasks;
	}
	
	public void applyDefaults(){
		for (Task t : getTasks()) t.applyDefaults(defaults);
	}
	
	public void applyDefaults(RedisHash rh){
		// set task information
				if (getInput().isEmpty()) {
					setInput(rh.get(TaskSetting.Input).toString());
				}
				if (getRunBeforeAsString().isEmpty()) {
					setRunBefore(rh.get(TaskSetting.RunBefore).toString());
				}
				if (getRunAfterAsString().isEmpty()) {
					setRunAfter(rh.get(TaskSetting.RunAfter).toString());
				}
				if (getTimeout() == null) {
					setTimeout(rh.get(TaskSetting.Timeout).toString());
				}
				if (getWaitAfterSetupError() == null) {
					setWaitAfterSetupError(rh.get(TaskSetting.WaitAfterSetupError).toString());
				}
				if (getWaitAfterRunError() == null) {
					setWaitAfterRunError(rh.get(TaskSetting.WaitAfterRunError).toString());
				}
				if (getWaitAfterSuccess() == null) {
					setWaitAfterSuccess(rh.get(TaskSetting.WaitAfterSuccess).toString());
				}

	}

	
}
