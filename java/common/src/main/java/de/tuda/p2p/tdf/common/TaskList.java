/**
 * 
 */
package de.tuda.p2p.tdf.common;

import java.io.FileNotFoundException;
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

	public TaskList(Jedis jedis, String namespace, Long index) throws FileNotFoundException {
		this(jedis, namespace);
		setIndex(index);
		load(jedis, namespace, index);
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
		if (task != null && tasks.add(task))
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

	public TaskList load(Jedis jedis, String namespace, Long index) throws FileNotFoundException {

		if (jedis == null || namespace == null || namespace == ""
				|| index == null) {
			return null;
		}
		if (!(jedis.exists(HashKey(namespace, index))||jedis.exists(SetKey(namespace, index)))) throw new FileNotFoundException("tasklist not found");
		setNamespace(namespace);
		setIndex(index);
		tasks.load(jedis, namespace, SetKey(namespace, index));
		defaults.load(jedis, HashKey(namespace, index));
		// load defaults
		return this;

	}

	public Long save(Jedis jedis, String namespace, Long index) {

		if (jedis == null || namespace == null || namespace == ""
				|| index == null) {
			return -1L;
		}
		setNamespace(namespace);
		setIndex(index);
		return save(jedis);

	}

	public Long save() {
		if (getNamespace() == null || getNamespace().isEmpty())
			return -1L;
		if (getIndex() == null) {
			setIndex((new Namespace(jedis, getNamespace())).getNewIndex());
		}
		System.out.println("set index: "+this.getIndex().toString());
		
		tasks.save(jedis, SetKey(getNamespace(), getIndex()),getNamespace());
		defaults.save(jedis, HashKey(getNamespace(), getIndex()));
		// save defaults
		return getIndex();
	}

	public String asString() {
		return this.asJsonString();
	}

	public Long save(Jedis jedis) {
		this.jedis=jedis;
		return save();

	}

	public Long save(Jedis jedis, String namespace) {
		setNamespace(namespace);
		return this.save(jedis);
	}

	public String getWorker() {
		return defaults.get(TaskSetting.Worker) == null ? null : defaults.get(
				TaskSetting.Worker).toString();
	}

	public void setWorker(String worker) {
		defaults.put(TaskSetting.Worker, worker);
	}

	public String getInput() {
		return defaults.get(TaskSetting.Input) == null ? null : defaults.get(
				TaskSetting.Input).toString();
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
		return getStarted() == null ? null : getStarted().toString();
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
		return getFinished() == null ? null : getFinished().toString();
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
		return getRunBefore() == null ? null : getRunBefore().toString();
	}

	public DateTime getRunBefore() {
		return defaults.get(TaskSetting.RunBefore) == null ? null : DateTime
				.parse(defaults.get(TaskSetting.RunBefore).toString());
	}

	public void setRunBefore(String runBefore) {
		defaults.put(TaskSetting.RunBefore, runBefore);

	}

	public void setRunBefore(DateTime runBefore) {
		setRunBefore(runBefore.toString());
	}

	public String getRunAfterAsString() {
		return getRunAfter() == null ? null : getRunAfter().toString();
	}

	public DateTime getRunAfter() {
		return defaults.get(TaskSetting.RunAfter) == null ? null : DateTime
				.parse(defaults.get(TaskSetting.RunAfter).toString());

	}

	public void setRunAfter(String runAfter) {
		defaults.put(TaskSetting.RunAfter, runAfter);

	}

	public void setRunAfter(DateTime runAfter) {
		setRunAfter(runAfter.toString());
	}

	public Integer getTimeout() {
		return defaults.get(TaskSetting.Timeout) == null ? null : Integer
				.valueOf(defaults.get(TaskSetting.Timeout).toString());
	}

	public void setTimeout(Integer timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public void setTimeout(String timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public Integer getWaitAfterSuccess() {
		return defaults.get(TaskSetting.WaitAfterSuccess) == null ? null
				: Integer.valueOf(defaults.get(TaskSetting.WaitAfterSuccess)
						.toString());

	}

	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
	}

	public void setWaitAfterSuccess(String waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);

	}

	public Integer getWaitAfterSetupError() {
		return defaults.get(TaskSetting.WaitAfterSetupError) == null ? null
				: Integer.valueOf(defaults.get(TaskSetting.WaitAfterSetupError)
						.toString());

	}

	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public void setWaitAfterSetupError(String waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public Integer getWaitAfterRunError() {
		return defaults.get(TaskSetting.WaitAfterRunError) == null ? null
				: Integer.valueOf(defaults.get(TaskSetting.WaitAfterRunError)
						.toString());

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
		return defaults.get(TaskSetting.Session) == null ? null : defaults.get(
				TaskSetting.Session).toString();
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
		return (getFinished() != null);
	}

	public void start(String client) {
		setClient(client);
		setStarted(DateTime.now());
	}

	public RedisTaskSet getTasks() {
		return tasks;
	}

	public RedisTaskSet getOpenTasks() {
		RedisTaskSet tasks = new RedisTaskSet(getTasks());
		for (Task task : tasks.toArray(new Task[0])) {
			task.load();
			if (task.isFinished())
				tasks.remove(task);
		}
		return tasks;
	}

	public void applyDefaults() {
		for (Task t : getTasks())
			t.applyDefaults(defaults);
	}

	public void applyDefaults(RedisHash rh) {
		// set task information
		if (getInput() != null && getInput().isEmpty()) {
			if (rh.get(TaskSetting.Input) != null)
				setInput(rh.get(TaskSetting.Input).toString());
		}
		if (getRunBeforeAsString() != null && getRunBeforeAsString().isEmpty()) {
			if (rh.get(TaskSetting.RunBefore) != null)
				setRunBefore(rh.get(TaskSetting.RunBefore).toString());
		}
		if (getRunAfterAsString() != null && getRunAfterAsString().isEmpty()) {
			if (rh.get(TaskSetting.RunAfter) != null)
				setRunAfter(rh.get(TaskSetting.RunAfter).toString());
		}
		if (getTimeout() == null) {
			if (rh.get(TaskSetting.Timeout) != null)
				setTimeout(rh.get(TaskSetting.Timeout).toString());
		}
		if (getWaitAfterSetupError() == null) {
			if (rh.get(TaskSetting.WaitAfterSetupError) != null)
				setWaitAfterSetupError(rh.get(TaskSetting.WaitAfterSetupError)
						.toString());
		}
		if (getWaitAfterRunError() == null) {
			if (rh.get(TaskSetting.WaitAfterRunError) != null)
				setWaitAfterRunError(rh.get(TaskSetting.WaitAfterRunError)
						.toString());
		}
		if (getWaitAfterSuccess() == null) {
			if (rh.get(TaskSetting.WaitAfterSuccess) != null)
				setWaitAfterSuccess(rh.get(TaskSetting.WaitAfterSuccess)
						.toString());
		}

	}

	public String asJsonString() {
		StringBuilder sb = new StringBuilder("{\n");
		if (getWorker() != null)
			sb.append("\"worker\": \"").append(getWorker()).append("\",\n");
		if (getClient() != null)
			sb.append("\"client: \"").append(getClient()).append("\",\n");
		if (getStarted() != null)
			sb.append("\"started: \"").append(getStarted()).append("\",\n");
		if (getFinished() != null)
			sb.append("\"finished: \"").append(getFinished()).append("\",\n");
		if (getRunAfter() != null)
			sb.append("\"runAfter: \"").append(getRunAfter()).append("\",\n");
		if (getRunBefore() != null)
			sb.append("\"runBefore: \"").append(getRunBefore()).append("\",\n");
		if (getTimeout() != null)
			sb.append("\"timeout: \"").append(getTimeout()).append("\",\n");
		if (getWaitAfterSuccess() != null)
			sb.append("\"waitAfterSuccess: \"").append(getWaitAfterSuccess())
					.append("\",\n");
		if (getWaitAfterSetupError() != null)
			sb.append("\"waitAfterSetupError: \"")
					.append(getWaitAfterSetupError()).append("\",\n");
		if (getWaitAfterRunError() != null)
			sb.append("\"waitAfterRunError: \"").append(getWaitAfterRunError())
					.append("\",\n");
		if (getIndex() != null)
			sb.append("\"index: \"").append(getIndex()).append("\",\n");
		if (getNamespace() != null)
			sb.append("\"namespace: \"").append(getNamespace()).append("\",\n");
		if (getSession() != null)
			sb.append("\"session: \"").append(getSession()).append("\"\n");
		sb.append("}");
		return sb.toString();
	}

	public void requeue() {
		System.out.println("Requeueing");
		// remove task from the running set
		jedis.srem("tdf." + getNamespace() + ".running", getIndex().toString());

		// remove client and started information
		jedis.hdel(HashKey(getNamespace(), getIndex()), "started");
		jedis.hdel(HashKey(getNamespace(), getIndex()), "client");

		// add task to the front of the queuing list
		jedis.lpush("tdf." + getNamespace() + ".queuinglists", getIndex().toString());
		// TODO Auto-generated method stub
		for(Task task: getTasks()) task.requeue(jedis);
		
	}

	public void deltask(Task t) {
		tasks.remove(t);
		
	}

}
