package de.tuda.p2p.tdf.common;

import java.io.FileNotFoundException;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import redis.clients.jedis.Jedis;

public class Task implements TaskLike {

	private RedisHash settings = new RedisHash();

	DateTimeFormatter formatter = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss");

	private String output;

	private String log;

	private String error;

	private String client;

	private DateTime started;

	private DateTime finished;

	private Long index;

	private String namespace;

	private String session;

	private Jedis jedis;

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#asString()
	 */
	@Override
	public String asString() {
		StringBuilder sb = new StringBuilder();
		sb.append("worker: ").append(getWorker()).append("\n");
		sb.append("client: ").append(getClient()).append("\n");
		sb.append("started: ").append(getStarted()).append("\n");
		sb.append("finished: ").append(getFinished()).append("\n");
		sb.append("runAfter: ").append(getRunAfter()).append("\n");
		sb.append("runBefore: ").append(getRunBefore()).append("\n");
		sb.append("timeout: ").append(getTimeout()).append("\n");
		sb.append("waitAfterSuccess: ").append(getWaitAfterSuccess())
				.append("\n");
		sb.append("waitAfterSetupError: ").append(getWaitAfterSetupError())
				.append("\n");
		sb.append("waitAfterRunError: ").append(getWaitAfterRunError())
				.append("\n");
		sb.append("index: ").append(getIndex()).append("\n");
		sb.append("namespace: ").append(getNamespace()).append("\n");
		sb.append("session: ").append(getSession()).append("\n");
		return sb.toString();
	}
	
	public String asJsonString(){
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

	/**
	 * Constructor that fills itself from Redis
	 * 
	 * @param jedis
	 *            The Jedis instance
	 * @param namespace
	 *            The namespace of the task
	 * @param index
	 *            The index of the task
	 * @throws FileNotFoundException 
	 */
	public Task(Jedis jedis, String namespace, Long index) throws FileNotFoundException {
		if (!jedis.exists(HashKey(namespace, index))) 
			throw new FileNotFoundException("task not found");
		String hashKey = HashKey(namespace, index);
		setIndex(index);
		setNamespace(namespace);

		setClient(jedis.hget(hashKey, "client"));
		setError(jedis.hget(hashKey, "error"));
		setRunBefore(jedis.hget(hashKey, "runBefore"));
		setRunAfter(jedis.hget(hashKey, "runAfter"));
		setFinished(jedis.hget(hashKey, "finished"));
		setInput(jedis.hget(hashKey, "input"));
		setLog(jedis.hget(hashKey, "log"));
		setOutput(jedis.hget(hashKey, "output"));
		setTimeout(jedis.hget(hashKey, "timeout"));
		setWaitAfterSetupError(jedis.hget(hashKey, "waitAfterSetupError"));
		setWaitAfterRunError(jedis.hget(hashKey, "waitAfterRunError"));
		setWaitAfterSuccess(jedis.hget(hashKey, "waitAfterSuccess"));
		setWorker(jedis.hget(hashKey, "worker"));
		setStarted(jedis.hget(hashKey, "started"));
		setSession(jedis.hget(hashKey, "session"));
		setJedis(jedis);
	}

	private void setJedis(Jedis jedis) {
		this.jedis=jedis;
		
	}

	public Task() {
	}

	/**
	 * Constructor with all attributes that can be set server-side
	 */
	public Task(String worker, String input, DateTime runBefore,
			DateTime runAfter, String timeout, String waitAfterSetupError,
			String waitAfterRunError, String waitAfterSuccess, String session) {
		setWorker(worker);
		setInput(input);
		setRunBefore(runBefore);
		setRunBefore(runAfter);
		setTimeout(timeout);
		setWaitAfterSetupError(waitAfterSetupError);
		setWaitAfterRunError(waitAfterRunError);
		setWaitAfterSuccess(waitAfterSuccess);
		setSession(session);
	}

	public Task(RedisHash rh) {

		for (Entry<TaskSetting, Object> e : rh.entrySet()) {
			switch (e.getKey()) {
			case Input:
				setInput(e.getValue().toString());
				break;
			case RunAfter:
				setRunAfter(e.getValue().toString());
				break;
			case RunBefore:
				setRunBefore(e.getValue().toString());
				break;
			case Session:
				setSession(e.getValue().toString());
				break;
			case Timeout:
				setTimeout(e.getValue().toString());
				break;
			case WaitAfterRunError:
				setWaitAfterRunError(e.getValue().toString());
				break;
			case WaitAfterSetupError:
				setWaitAfterSetupError(e.getValue().toString());
				break;
			case WaitAfterSuccess:
				setWaitAfterSuccess(e.getValue().toString());
				break;
			case Worker:
				setWorker(e.getValue().toString());
				break;
			default:
				break;

			}
		}

	}

	public Task(Task task) {
		
	}

	/**
	 * Saves the task using the task's namespace and index field. If a task with
	 * this index exists, it will be updated/overwritten.
	 * 
	 * @param jedis
	 *            The jedis instance
	 * @return The task's index, if successful, -1 if not (e.g. the task
	 *         namespace or index field is not set)
	 */
	public Long save(Jedis jedis) {
		if (getNamespace() == null || getNamespace().isEmpty()) {
			return -1L;
		}
		return this.save(jedis, getNamespace());
	}

	/**
	 * Saves the task using the task's index field. If a task with this index
	 * exists, it will be updated/overwritten.
	 * 
	 * @param jedis
	 *            The Jedis instance
	 * @param namespace
	 *            The namespace of the task
	 * @return The task's index, if successful, -1 if not (e.g. the task index
	 *         field is not set)
	 */
	public Long save(Jedis jedis, String namespace) {
		if (getIndex() == null) {
			setIndex((new Namespace(jedis, namespace)).getNewIndex());
		}
		return this.save(jedis, namespace, getIndex());
	}

	/**
	 * Saves the task. If a task with the given index exists, it will be
	 * updated/overwritten.
	 * 
	 * @param jedis
	 *            The Jedis instance
	 * @param namespace
	 *            The namespace of the task
	 * @param index
	 *            The index of the task
	 * @return The task's index, if successful, -1 if not
	 */
	public Long save(Jedis jedis, String namespace, Long index) {
		if (getWorker().isEmpty()) {
			return -1L;
		}

		setNamespace(namespace);
		setIndex(index);

		String hashKey = HashKey(namespace, index);

		// set worker information for task
		jedis.hset(hashKey, "worker", getWorker());

		// set task information
		if (getInput() != null &&!getInput().isEmpty()) {
			jedis.hset(hashKey, "input", getInput());
		}
		if ( getRunBeforeAsString() != null && !getRunBeforeAsString().isEmpty()) {
			jedis.hset(hashKey, "runBefore", getRunBeforeAsString());
		}
		if (getRunAfterAsString() != null && !getRunAfterAsString().isEmpty()) {
			jedis.hset(hashKey, "runAfter", getRunAfterAsString());
		}
		if (getTimeout() != null) {
			jedis.hset(hashKey, "timeout", getTimeout().toString());
		}
		if (getWaitAfterSetupError() != null) {
			jedis.hset(hashKey, "waitAfterSetupError", getWaitAfterSetupError()
					.toString());
		}
		if (getWaitAfterRunError() != null) {
			jedis.hset(hashKey, "waitAfterRunError", getWaitAfterRunError()
					.toString());
		}
		if (getWaitAfterSuccess() != null) {
			jedis.hset(hashKey, "waitAfterSuccess", getWaitAfterSuccess()
					.toString());
		}
		if (getSession() != null) {
			jedis.hset(hashKey, "session", getSession());
		}

		return index;
	}

	private String HashKey(String namespace, Long index) {
		return "tdf." + namespace + ".task." + index;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getWorker()
	 */
	@Override
	public String getWorker() {
		return getsetting(TaskSetting.Worker);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#setWorker(java.lang.String)
	 */
	@Override
	public void setWorker(String worker) {
		if (worker != null) {
			settings.put(TaskSetting.Worker, worker);
		} else {
			setWorker("");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getInput()
	 */
	@Override
	public String getInput() {
		return getsetting(TaskSetting.Input);
	}

	private String getsetting(TaskSetting input) {
		// TODO Auto-generated method stub
		return (settings.containsKey(input) ? settings.get(input).toString() : null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#setInput(java.lang.String)
	 */
	@Override
	public void setInput(String input) {
		if (input != null) {
			settings.put(TaskSetting.Input, input);
		} else {
			setInput("");
		}
	}

	public String getOutput() {
		return output;
	}

	/**
	 * For client use only
	 */
	void setOutput(String output) {
		if (output != null) {
			this.output = output;
		} else {
			this.output = "";
		}
	}

	public String getLog() {
		return log;
	}

	/**
	 * For client use only
	 */
	void setLog(String log) {
		if (log != null) {
			this.log = log;
		} else {
			this.log = "";
		}
	}

	public String getError() {
		return error;
	}

	/**
	 * For client use only
	 */
	void setError(String error) {
		if (error != null) {
			this.error = error;
		} else {
			this.error = "";
		}
	}

	public String getClient() {
		return client;
	}

	/**
	 * For client use only
	 */
	void setClient(String client) {
		if (client != null) {
			this.client = client;
		} else {
			this.client = "";
		}
	}

	public String getStartedAsString() {
		if (getStarted() == null) {
			return "";
		}
		return getStarted().toString(formatter);
	}

	public DateTime getStarted() {
		return started;
	}

	/**
	 * For client use only
	 */
	void setStarted(DateTime started) {
		this.started = started;
	}

	/**
	 * For client use only
	 */
	void setStarted(String started) {
		if (started != null && !started.isEmpty()) {
			this.setStarted(formatter.parseDateTime(started));
		}
	}

	public String getFinishedAsString() {
		if (getFinished() == null) {
			return "";
		}
		return getFinished().toString(formatter);
	}

	public DateTime getFinished() {
		return finished;
	}

	/**
	 * For client use only
	 */
	void setFinished(DateTime finished) {
		this.finished = finished;
	}

	/**
	 * For client use only
	 */
	void setFinished(String finished) {
		if (finished != null && !finished.isEmpty()) {
			this.setFinished(formatter.parseDateTime(finished));
		}
	}

	public String getRunBeforeAsString() {
		if (getRunBefore() == null) {
			return "";
		}
		return getRunBefore().toString(formatter);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getRunBefore()
	 */
	@Override
	public DateTime getRunBefore() {
		if (getsetting(TaskSetting.RunBefore) == null)
			return null;
		return formatter.parseDateTime(getsetting(TaskSetting.RunBefore));

	}

	public void setRunBefore(String runBefore) {
		if (runBefore != null && !runBefore.isEmpty()) {
			settings.put(TaskSetting.RunBefore, runBefore);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#setRunBefore(org.joda.time.DateTime)
	 */
	@Override
	public void setRunBefore(DateTime runBefore) {
		setRunBefore(runBefore.toString(formatter));
	}

	public String getRunAfterAsString() {
		if (getRunAfter() == null) {
			return "";
		}
		return getRunAfter().toString(formatter);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getRunAfter()
	 */
	@Override
	public DateTime getRunAfter() {
		if (getsetting(TaskSetting.RunAfter) == null)
			return null;
		return formatter.parseDateTime(getsetting(TaskSetting.RunAfter));

	}

	public void setRunAfter(String runAfter) {
		if (runAfter != null && !runAfter.isEmpty()) {
			settings.put(TaskSetting.RunAfter, runAfter);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#setRunAfter(org.joda.time.DateTime)
	 */
	@Override
	public void setRunAfter(DateTime runAfter) {
		setRunAfter(runAfter.toString(formatter));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getTimeout()
	 */
	@Override
	public Integer getTimeout() {
		if (getsetting(TaskSetting.Timeout) == null)
			return null;
		return Integer.parseInt(getsetting(TaskSetting.Timeout));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#setTimeout(java.lang.Integer)
	 */
	@Override
	public void setTimeout(Integer timeout) {
		setTimeout(timeout.toString());
	}

	public void setTimeout(String timeout) {
		if (timeout != null && !timeout.isEmpty()) {
			settings.put(TaskSetting.Timeout, timeout);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getWaitAfterSuccess()
	 */
	@Override
	public Integer getWaitAfterSuccess() {
		if (getsetting(TaskSetting.WaitAfterSuccess) == null)
			return null;
		return Integer.valueOf(getsetting(TaskSetting.WaitAfterSuccess));

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuda.p2p.tdf.common.TaskLike#setWaitAfterSuccess(java.lang.Integer)
	 */
	@Override
	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		settings.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
	}

	public void setWaitAfterSuccess(String waitAfterSuccess) {
		if (waitAfterSuccess != null && !waitAfterSuccess.isEmpty()) {
			settings.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getWaitAfterSetupError()
	 */
	@Override
	public Integer getWaitAfterSetupError() {
		if (getsetting(TaskSetting.WaitAfterSetupError) == null)
			return null;
		return Integer.valueOf(getsetting(TaskSetting.WaitAfterSetupError));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuda.p2p.tdf.common.TaskLike#setWaitAfterSetupError(java.lang.Integer)
	 */
	@Override
	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		settings.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public void setWaitAfterSetupError(String waitAfterSetupError) {
		if (waitAfterSetupError != null && !waitAfterSetupError.isEmpty()) {
			settings.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuda.p2p.tdf.common.TaskLike#getWaitAfterRunError()
	 */
	@Override
	public Integer getWaitAfterRunError() {
		if (getsetting(TaskSetting.WaitAfterRunError) == null)
			return null;
		return Integer.valueOf(getsetting(TaskSetting.WaitAfterRunError));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuda.p2p.tdf.common.TaskLike#setWaitAfterRunError(java.lang.Integer)
	 */
	@Override
	public void setWaitAfterRunError(Integer waitAfterRunError) {
		settings.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public void setWaitAfterRunError(String waitAfterRunError) {
		if (waitAfterRunError != null && !waitAfterRunError.isEmpty()) {
			settings.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
		}
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
		if (session == null || session.isEmpty()) {
			return DateTime.now().toString("yyyy-MM-dd");
		}
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	/**
	 * Decides, if this task should still be executed
	 * 
	 * @return true if this task is expired, false if it is still valid
	 */
	public boolean isExpired() {
		if (getRunBefore() == null) {
			return false;
		}
		return getRunBefore().isBeforeNow();
	}

	/**
	 * Decides, if this task can be executed already
	 * 
	 * @return true if this task can be executed, false if it is not valid yet
	 */
	public boolean isValid() {
		if (getRunAfter() == null) {
			return true;
		}
		return getRunAfter().isBeforeNow();
	}

	/**
	 * Return the time to wait before the task is valid
	 * 
	 * @return The time to wait in milliseconds
	 */
	public Long validWaitTime() {
		Long waitTime = getRunAfter().getMillis() - DateTime.now().getMillis();
		return (waitTime > 0) ? waitTime : 0;
	}

	/**
	 * Decides if this task should be requeued
	 * 
	 * @return true if the task should be requeued, false if not
	 */
	public boolean isTimedOut() {
		if (getStarted() == null || getTimeout() == null) {
			return false;
		}
		if (getRunAfter() == null) {
			return getStarted().plusMillis(getTimeout()).isBeforeNow();
		}
		return getRunAfter().plusMillis(getTimeout()).isBeforeNow();
	}

	/**
	 * Returns whether the task is started or not
	 * 
	 * @return true if started, false if not
	 */
	public boolean isStarted() {
		return (getStarted() != null);
	}

	/**
	 * Returns whether the task is finished or not
	 * 
	 * @return true if finished, false if not
	 */
	public boolean isFinished() {
		return (getFinished() != null);
	}

	/**
	 * "Starts" a task by setting the client id and the started attribute to
	 * "now". Used for testing purposes.
	 * 
	 * @param client
	 *            The client that executes the task
	 */
	public void start(String client) {
		this.setStarted(DateTime.now());
		setClient(client);
	}

	public void applyDefaults(RedisHash rh) {
		// set task information
		if (getInput() == null || getInput().isEmpty()) {
			if (rh.containsKey(TaskSetting.Input))
				setInput(rh.get(TaskSetting.Input).toString());
		}
		if (getRunBeforeAsString() == null || getRunBeforeAsString().isEmpty()) {
			if (rh.containsKey(TaskSetting.RunBefore))
				setRunBefore(rh.get(TaskSetting.RunBefore).toString());
		}
		if (getRunAfterAsString() == null || getRunAfterAsString().isEmpty()) {
			if (rh.containsKey(TaskSetting.RunAfter))
				setRunAfter(rh.get(TaskSetting.RunAfter).toString());
		}
		if (getTimeout() == null) {
			if (rh.containsKey(TaskSetting.Timeout))
				setTimeout(rh.get(TaskSetting.Timeout).toString());
		}
		if (getWaitAfterSetupError() == null) {
			if (rh.containsKey(TaskSetting.WaitAfterSetupError))
				setWaitAfterSetupError(rh.get(TaskSetting.WaitAfterSetupError)
						.toString());
		}
		if (getWaitAfterRunError() == null) {
			if (rh.containsKey(TaskSetting.WaitAfterRunError))
				setWaitAfterRunError(rh.get(TaskSetting.WaitAfterRunError)
						.toString());
		}
		if (getWaitAfterSuccess() == null) {
			if (rh.containsKey(TaskSetting.WaitAfterSuccess))
				setWaitAfterSuccess(rh.get(TaskSetting.WaitAfterSuccess)
						.toString());
		}
		if (getWorker() == null) {
			if (rh.containsKey(TaskSetting.Worker))
				setWorker(rh.get(TaskSetting.Worker)
						.toString());
		}

	}

	public Jedis getJedis() {
		return jedis;
	}

	public void requeue() {
		if(getJedis() != null && getJedis().isConnected())
			requeue(getJedis());
	}
	
	public void requeue(Jedis jedis) {
		jedis.srem("tdf." + getNamespace() + ".running", getIndex().toString());

		// remove client and started information
		jedis.hdel(HashKey(getNamespace(), getIndex()), "started");
		jedis.hdel(HashKey(getNamespace(), getIndex()), "client");

		// add task to the front of the queuing list
		jedis.lpush("tdf." + getNamespace() + ".queuing", getIndex().toString());
		// TODO Auto-generated method stub
	}

}
