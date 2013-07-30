package de.tuda.p2p.tdf.common;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import redis.clients.jedis.Jedis;

public class Task {

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

	/**
	 * @return a text representation of all the task's information except input,
	 *         output, log and error
	 */
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
	public Task(Jedis jedis, String namespace, Long index) {
		String hashKey = "tdf." + namespace + ".task." + index;
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

	public Task() {
	}

	/**
	 * Constructor with all attributes that can be set server-side
	 */
	public Task(String worker, String input,
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
			return -1L;
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

		String hashKey = "tdf." + namespace + ".task." + index;

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

	public String getWorker() {
		return worker;
	}

	public void setWorker(String worker) {
		if (worker != null) {
			this.worker = worker;
		} else {
			this.worker = "";
		}
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		if (input != null) {
			this.input = input;
		} else {
			this.input = "";
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
		if (started == null) {
			return "";
		}
		return started.toString(formatter);
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
		if (finished == null) {
			return "";
		}
		return finished.toString(formatter);
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
		if (runBefore == null) {
			return "";
		}
		return runBefore.toString(formatter);
	}

	public DateTime getRunBefore() {
		return runBefore;
	}

	public void setRunBefore(String runBefore) {
		if (runBefore != null && !runBefore.isEmpty()) {
			this.runBefore = formatter.parseDateTime(runBefore);
		}
	}

	public void setRunBefore(DateTime runBefore) {
		this.runBefore = runBefore;
	}

	public String getRunAfterAsString() {
		if (runAfter == null) {
			return "";
		}
		return runAfter.toString(formatter);
	}

	public DateTime getRunAfter() {
		return runAfter;
	}

	public void setRunAfter(String runAfter) {
		if (runAfter != null && !runAfter.isEmpty()) {
			this.runAfter = formatter.parseDateTime(runAfter);
		}
	}

	public void setRunAfter(DateTime runAfter) {
		this.runAfter = runAfter;
	}

	public Integer getTimeout() {
		return timeout;
	}

	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}

	public void setTimeout(String timeout) {
		if (timeout != null && !timeout.isEmpty()) {
			this.timeout = Integer.valueOf(timeout);
		}
	}

	public Integer getWaitAfterSuccess() {
		return waitAfterSuccess;
	}

	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		this.waitAfterSuccess = waitAfterSuccess;
	}

	public void setWaitAfterSuccess(String waitAfterSuccess) {
		if (waitAfterSuccess != null && !waitAfterSuccess.isEmpty()) {
			this.waitAfterSuccess = Integer.valueOf(waitAfterSuccess);
		}
	}

	public Integer getWaitAfterSetupError() {
		return waitAfterSetupError;
	}

	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		this.waitAfterSetupError = waitAfterSetupError;
	}

	public void setWaitAfterSetupError(String waitAfterSetupError) {
		if (waitAfterSetupError != null && !waitAfterSetupError.isEmpty()) {
			this.waitAfterSetupError = Integer.valueOf(waitAfterSetupError);
		}
	}

	public Integer getWaitAfterRunError() {
		return waitAfterRunError;
	}

	public void setWaitAfterRunError(Integer waitAfterRunError) {
		this.waitAfterRunError = waitAfterRunError;
	}

	public void setWaitAfterRunError(String waitAfterRunError) {
		if (waitAfterRunError != null && !waitAfterRunError.isEmpty()) {
			this.waitAfterRunError = Integer.valueOf(waitAfterRunError);
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
		if (runBefore == null) {
			return false;
		}
		return runBefore.isBeforeNow();
	}

	/**
	 * Decides, if this task can be executed already
	 *
	 * @return true if this task can be executed, false if it is not valid yet
	 */
	public boolean isValid() {
		if (runAfter == null) {
			return true;
		}
		return runAfter.isBeforeNow();
	}

	/**
	 * Return the time to wait before the task is valid
	 *
	 * @return The time to wait in milliseconds
	 */
	public Long validWaitTime() {
		Long waitTime = runAfter.getMillis() - DateTime.now().getMillis();
		return (waitTime > 0) ? waitTime : 0;
	}

	/**
	 * Decides if this task should be requeued
	 *
	 * @return true if the task should be requeued, false if not
	 */
	public boolean isTimedOut() {
		if (started == null || timeout == null) {
			return false;
		}
		if (runAfter == null) {
			return started.plusMillis(timeout).isBeforeNow();
		}
		return runAfter.plusMillis(timeout).isBeforeNow();
	}

	/**
	 * Returns whether the task is started or not
	 *
	 * @return true if started, false if not
	 */
	public boolean isStarted() {
		return (started != null);
	}

	/**
	 * Returns whether the task is finished or not
	 *
	 * @return true if finished, false if not
	 */
	public boolean isFinished() {
		return (finished != null);
	}

	/**
	 * "Starts" a task by setting the client id and the started attribute to "now". Used for
	 * testing purposes.
	 *
	 * @param client
	 *            The client that executes the task
	 */
	public void start(String client) {
		this.started = DateTime.now();
		setClient(client);
	}

}
