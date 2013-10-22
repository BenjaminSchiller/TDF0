package de.tuda.p2p.tdf2.common.interfaces;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;

public interface TaskInterface {

	/**
	 * @return a text representation of all the task's information except input,
	 *         output, log and error
	 */
	public abstract String asString();

	/**
	 * Saves the task using the task's namespace and index field. If a task with
	 * this index exists, it will be updated/overwritten.
	 *
	 * @param jedis
	 *            The jedis instance
	 * @return The task's index, if successful, -1 if not (e.g. the task
	 *         namespace or index field is not set)
	 */
	public abstract Long save(Jedis jedis);

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
	public abstract Long save(Jedis jedis, String namespace);

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
	public abstract Long save(Jedis jedis, String namespace, Long index);

	public abstract String getWorker();

	public abstract void setWorker(String worker);

	public abstract String getInput();

	public abstract void setInput(String input);

	public abstract String getOutput();

	public abstract String getLog();

	public abstract String getError();

	public abstract String getClient();

	public abstract String getStartedAsString();

	public abstract DateTime getStarted();

	public abstract String getFinishedAsString();

	public abstract DateTime getFinished();

	public abstract String getRunBeforeAsString();

	public abstract DateTime getRunBefore();

	public abstract void setRunBefore(String runBefore);

	public abstract void setRunBefore(DateTime runBefore);

	public abstract String getRunAfterAsString();

	public abstract DateTime getRunAfter();

	public abstract void setRunAfter(String runAfter);

	public abstract void setRunAfter(DateTime runAfter);

	public abstract Integer getTimeout();

	public abstract void setTimeout(Integer timeout);

	public abstract void setTimeout(String timeout);

	public abstract Integer getWaitAfterSuccess();

	public abstract void setWaitAfterSuccess(Integer waitAfterSuccess);

	public abstract void setWaitAfterSuccess(String waitAfterSuccess);

	public abstract Integer getWaitAfterSetupError();

	public abstract void setWaitAfterSetupError(Integer waitAfterSetupError);

	public abstract void setWaitAfterSetupError(String waitAfterSetupError);

	public abstract Integer getWaitAfterRunError();

	public abstract void setWaitAfterRunError(Integer waitAfterRunError);

	public abstract void setWaitAfterRunError(String waitAfterRunError);

	public abstract Long getIndex();

	public abstract void setIndex(Long index);

	public abstract String getNamespace();

	public abstract void setNamespace(String namespace);

	public abstract String getSession();

	public abstract void setSession(String session);

	/**
	 * Decides, if this task should still be executed
	 *
	 * @return true if this task is expired, false if it is still valid
	 */
	public abstract boolean isExpired();

	/**
	 * Decides, if this task can be executed already
	 *
	 * @return true if this task can be executed, false if it is not valid yet
	 */
	public abstract boolean isValid();

	/**
	 * Return the time to wait before the task is valid
	 *
	 * @return The time to wait in milliseconds
	 */
	public abstract Long validWaitTime();

	/**
	 * Decides if this task should be requeued
	 *
	 * @return true if the task should be requeued, false if not
	 */
	public abstract boolean isTimedOut();

	/**
	 * Returns whether the task is started or not
	 *
	 * @return true if started, false if not
	 */
	public abstract boolean isStarted();

	/**
	 * Returns whether the task is finished or not
	 *
	 * @return true if finished, false if not
	 */
	public abstract boolean isFinished();

	/**
	 * "Starts" a task by setting the client id and the started attribute to "now". Used for
	 * testing purposes.
	 *
	 * @param client
	 *            The client that executes the task
	 */
	public abstract void start(String client);

}