package de.tuda.p2p.tdf2.common;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import de.tuda.p2p.tdf2.common.interfaces.TaskInterface;
import redis.clients.jedis.Jedis;

public class OldTask implements TaskInterface {

	// the location of the worker (including version), that can execute this
	// task (http-URI, https-URI or file-URI)
	private String worker = "";

	// the input string, that will be handed to the worker
	private String input = "";

	// the output-string, returned from the worker
	private String output = "";

	// the logged STDOUT from the worker
	private String log = "";

	// the logged STDERR from the worker
	private String error = "";

	// the client-id that executes/executed the task
	private String client = "";

	// the timestamp when this task was started
	private DateTime started;

	// the timestamp when this task was finished
	private DateTime finished;

	// the timestamp when this task expires
	private DateTime runBefore;

	// the timestamp after this task is valid
	private DateTime runAfter;

	// the time in milliseconds after this task should be requeued (from the server) and cancelled (from the client).
	private Integer timeout;

	// the time in milliseconds to wait before executing the next task after
	// success
	private Integer waitAfterSuccess;

	// the time in milliseconds to wait before executing the next task after
	// failure during task execution
	private Integer waitAfterRunError;

	// the time in milliseconds to wait before executing the next task after
	// failure during task setup
	private Integer waitAfterSetupError;

	// the task index in the database
	private Long index;

	// the namespace of the task
	private String namespace;

	// the session of the task
	private String session;

	DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#asString()
	 */
	@Override
	public String asString() {
		StringBuilder sb = new StringBuilder();
		sb.append("worker: ").append(worker).append("\n");
		sb.append("client: ").append(client).append("\n");
		sb.append("started: ").append(started).append("\n");
		sb.append("finished: ").append(finished).append("\n");
		sb.append("runAfter: ").append(runAfter).append("\n");
		sb.append("runBefore: ").append(runBefore).append("\n");
		sb.append("timeout: ").append(timeout).append("\n");
		sb.append("waitAfterSuccess: ").append(waitAfterSuccess).append("\n");
		sb.append("waitAfterSetupError: ").append(waitAfterSetupError).append("\n");
		sb.append("waitAfterRunError: ").append(waitAfterRunError).append("\n");
		sb.append("index: ").append(index).append("\n");
		sb.append("namespace: ").append(namespace).append("\n");
		sb.append("session: ").append(session).append("\n");
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
	 */
	public OldTask(Jedis jedis, String namespace, Long index) {
		String hashKey = HashKey(namespace,index);
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
	}

	public OldTask() {
	}

	/**
	 * Constructor with all attributes that can be set server-side
	 */
	public OldTask(String worker, String input,
			DateTime runBefore, DateTime runAfter, String timeout,
			String waitAfterSetupError, String waitAfterRunError, 
			String waitAfterSuccess, String session) {
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#save(redis.clients.jedis.Jedis)
	 */
	@Override
	public Long save(Jedis jedis) {
		if (getNamespace() == null || getNamespace().isEmpty()) {
			return -1L;
		}
		return this.save(jedis, getNamespace());
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#save(redis.clients.jedis.Jedis, java.lang.String)
	 */
	@Override
	public Long save(Jedis jedis, String namespace) {
		if (getIndex() == null) {
			return -1L;
		}
		return this.save(jedis, namespace, getIndex());
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#save(redis.clients.jedis.Jedis, java.lang.String, java.lang.Long)
	 */
	@Override
	public Long save(Jedis jedis, String namespace, Long index) {
		if (getWorker().isEmpty()) {
			return -1L;
		}

		setNamespace(namespace);
		setIndex(index);

		String hashKey = HashKey(namespace,index);

		// set worker information for task
		jedis.hset(hashKey, "worker", getWorker());

		// set task information
		if (!getInput().isEmpty()) {
			jedis.hset(hashKey, "input", getInput());
		}
		if (!getRunBeforeAsString().isEmpty()) {
			jedis.hset(hashKey, "runBefore", getRunBeforeAsString());
		}
		if (!getRunAfterAsString().isEmpty()) {
			jedis.hset(hashKey, "runAfter", getRunAfterAsString());
		}
		if (getTimeout() != null) {
			jedis.hset(hashKey, "timeout", getTimeout().toString());
		}
		if (getWaitAfterSetupError() != null) {
			jedis.hset(hashKey, "waitAfterSetupError", getWaitAfterSetupError().toString());
		}
		if (getWaitAfterRunError() != null) {
			jedis.hset(hashKey, "waitAfterRunError", getWaitAfterRunError().toString());
		}
		if (getWaitAfterSuccess() != null) {
			jedis.hset(hashKey, "waitAfterSuccess", getWaitAfterSuccess().toString());
		}
		if (getSession() != null) {
			jedis.hset(hashKey, "session", getSession());
		}

		return index;
	}

	
	private String HashKey(String namespace, Long index) {
			return "tdf." + namespace + ".task." + index;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getWorker()
	 */
	@Override
	public String getWorker() {
		return worker;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWorker(java.lang.String)
	 */
	@Override
	public void setWorker(String worker) {
		if (worker != null) {
			this.worker = worker;
		} else {
			this.worker = "";
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getInput()
	 */
	@Override
	public String getInput() {
		return input;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setInput(java.lang.String)
	 */
	@Override
	public void setInput(String input) {
		if (input != null) {
			this.input = input;
		} else {
			this.input = "";
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getOutput()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getLog()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getError()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getClient()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getStartedAsString()
	 */
	@Override
	public String getStartedAsString() {
		if (started == null) {
			return "";
		}
		return started.toString(formatter);
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getStarted()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getFinishedAsString()
	 */
	@Override
	public String getFinishedAsString() {
		if (finished == null) {
			return "";
		}
		return finished.toString(formatter);
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getFinished()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getRunBeforeAsString()
	 */
	@Override
	public String getRunBeforeAsString() {
		if (runBefore == null) {
			return "";
		}
		return runBefore.toString(formatter);
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getRunBefore()
	 */
	@Override
	public DateTime getRunBefore() {
		return runBefore;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setRunBefore(java.lang.String)
	 */
	@Override
	public void setRunBefore(String runBefore) {
		if (runBefore != null && !runBefore.isEmpty()) {
			this.runBefore = formatter.parseDateTime(runBefore);
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setRunBefore(org.joda.time.DateTime)
	 */
	@Override
	public void setRunBefore(DateTime runBefore) {
		this.runBefore = runBefore;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getRunAfterAsString()
	 */
	@Override
	public String getRunAfterAsString() {
		if (runAfter == null) {
			return "";
		}
		return runAfter.toString(formatter);
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getRunAfter()
	 */
	@Override
	public DateTime getRunAfter() {
		return runAfter;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setRunAfter(java.lang.String)
	 */
	@Override
	public void setRunAfter(String runAfter) {
		if (runAfter != null && !runAfter.isEmpty()) {
			this.runAfter = formatter.parseDateTime(runAfter);
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setRunAfter(org.joda.time.DateTime)
	 */
	@Override
	public void setRunAfter(DateTime runAfter) {
		this.runAfter = runAfter;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getTimeout()
	 */
	@Override
	public Integer getTimeout() {
		return timeout;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setTimeout(java.lang.Integer)
	 */
	@Override
	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setTimeout(java.lang.String)
	 */
	@Override
	public void setTimeout(String timeout) {
		if (timeout != null && !timeout.isEmpty()) {
			this.timeout = Integer.valueOf(timeout);
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getWaitAfterSuccess()
	 */
	@Override
	public Integer getWaitAfterSuccess() {
		return waitAfterSuccess;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWaitAfterSuccess(java.lang.Integer)
	 */
	@Override
	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		this.waitAfterSuccess = waitAfterSuccess;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWaitAfterSuccess(java.lang.String)
	 */
	@Override
	public void setWaitAfterSuccess(String waitAfterSuccess) {
		if (waitAfterSuccess != null && !waitAfterSuccess.isEmpty()) {
			this.waitAfterSuccess = Integer.valueOf(waitAfterSuccess);
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getWaitAfterSetupError()
	 */
	@Override
	public Integer getWaitAfterSetupError() {
		return waitAfterSetupError;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWaitAfterSetupError(java.lang.Integer)
	 */
	@Override
	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		this.waitAfterSetupError = waitAfterSetupError;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWaitAfterSetupError(java.lang.String)
	 */
	@Override
	public void setWaitAfterSetupError(String waitAfterSetupError) {
		if (waitAfterSetupError != null && !waitAfterSetupError.isEmpty()) {
			this.waitAfterSetupError = Integer.valueOf(waitAfterSetupError);
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getWaitAfterRunError()
	 */
	@Override
	public Integer getWaitAfterRunError() {
		return waitAfterRunError;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWaitAfterRunError(java.lang.Integer)
	 */
	@Override
	public void setWaitAfterRunError(Integer waitAfterRunError) {
		this.waitAfterRunError = waitAfterRunError;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setWaitAfterRunError(java.lang.String)
	 */
	@Override
	public void setWaitAfterRunError(String waitAfterRunError) {
		if (waitAfterRunError != null && !waitAfterRunError.isEmpty()) {
			this.waitAfterRunError = Integer.valueOf(waitAfterRunError);
		}
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getIndex()
	 */
	@Override
	public Long getIndex() {
		return index;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setIndex(java.lang.Long)
	 */
	@Override
	public void setIndex(Long index) {
		this.index = index;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getNamespace()
	 */
	@Override
	public String getNamespace() {
		return namespace;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setNamespace(java.lang.String)
	 */
	@Override
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#getSession()
	 */
	@Override
	public String getSession() {
		if (session == null || session.isEmpty()) {
			return DateTime.now().toString("yyyy-MM-dd");
		}
		return session;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#setSession(java.lang.String)
	 */
	@Override
	public void setSession(String session) {
		this.session = session;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#isExpired()
	 */
	@Override
	public boolean isExpired() {
		if (runBefore == null) {
			return false;
		}
		return runBefore.isBeforeNow();
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#isValid()
	 */
	@Override
	public boolean isValid() {
		if (runAfter == null) {
			return true;
		}
		return runAfter.isBeforeNow();
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#validWaitTime()
	 */
	@Override
	public Long validWaitTime() {
		Long waitTime = runAfter.getMillis() - DateTime.now().getMillis();
		return (waitTime > 0) ? waitTime : 0;
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#isTimedOut()
	 */
	@Override
	public boolean isTimedOut() {
		if (started == null || timeout == null) {
			return false;
		}
		if (runAfter == null) {
			return started.plusMillis(timeout).isBeforeNow();
		}
		return runAfter.plusMillis(timeout).isBeforeNow();
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#isStarted()
	 */
	@Override
	public boolean isStarted() {
		return (started != null);
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#isFinished()
	 */
	@Override
	public boolean isFinished() {
		return (finished != null);
	}

	/* (non-Javadoc)
	 * @see de.tuda.p2p.tdf2.common.Task#start(java.lang.String)
	 */
	@Override
	public void start(String client) {
		this.started = DateTime.now();
		setClient(client);
	}

}
