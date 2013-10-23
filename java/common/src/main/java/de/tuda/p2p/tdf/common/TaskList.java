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
public class TaskList {

	private RedisTaskSet tasks = new RedisTaskSet();
	private String namespace;
	private Jedis jedis;
	private RedisHash defaults = new RedisHash();
	private String client;
	private DateTime started;
	private DateTime finished;
	private DateTime runBefore;
	private DateTime runAfter;
	private Long index;

	/**
	 * @param jedis
	 * @param namespace
	 * @param index
	 */
	public TaskList(Jedis jedis, String namespace) {
		this.setNamespace(namespace);
		this.setJedis(jedis);

		// TODO Auto-generated constructor stub
	}

	private void setJedis(Jedis jedis) {

		this.jedis = jedis;

	}

	/**
	 * 
	 */
	public TaskList() {
		// TODO Auto-generated constructor stub
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
		return runBefore;
	}

	public void setRunBefore(String runBefore) {
		this.runBefore = DateTime.parse(runBefore);

	}

	public void setRunBefore(DateTime runBefore) {
		this.runBefore = runBefore;
	}

	public String getRunAfterAsString() {
		return this.getRunAfter().toString();
	}

	public DateTime getRunAfter() {
		return runAfter;
	}

	public void setRunAfter(String runAfter) {
		this.runAfter = DateTime.parse(runAfter);

	}

	public void setRunAfter(DateTime runAfter) {
		this.runAfter = runAfter;
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
		if (runAfter == null)
			return true;
		return runAfter.isBeforeNow();
	}

	public Long validWaitTime() {
		Long waitTime = runAfter.getMillis() - DateTime.now().getMillis();
		return (waitTime > 0) ? waitTime : 0;
	}

	public boolean isTimedOut() {
		if (started == null || getTimeout() == null)
			return false;

		if (runAfter == null)
			return started.plusMillis(getTimeout()).isBeforeNow();

		return runAfter.plusMillis(getTimeout()).isBeforeNow();

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
}
