package de.tuda.p2p.tdf.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.Task;

import redis.clients.jedis.Jedis;

public class TaskInterfaceServer {

	private Jedis jedis;

	/**
	 * Constructor without authentication
	 *
	 * @param host
	 *            The hostname / IP address of the Redis database
	 * @param port
	 *            The port of the Redis database
	 * @param index
	 *            The database index
	 */
	public TaskInterfaceServer(String host, Integer port, Integer index) {
		jedis = new Jedis(host, port);
		jedis.select(index);
	}

	/**
	 * Constructor with authentication
	 *
	 * @param host
	 *            The hostname / IP address of the Redis database
	 * @param port
	 *            The port of the Redis database
	 * @param index
	 *            The database index
	 * @param password
	 *            The password for authentication
	 */
	public TaskInterfaceServer(String host, Integer port, Integer index,
			String password) {
		jedis = new Jedis(host, port);
		jedis.auth(password);
		jedis.select(index);
	}

	/**
	 * Gives access to the Redis library directly
	 *
	 * @return The Jedis object
	 */
	public Jedis getJedis() {
		return jedis;
	}

	/**
	 * Returns a set of all namespaces
	 *
	 * @return The names of all namespaces
	 */
	public Set<String> getNamespaces() {
		return jedis.smembers("tdf.namespaces");
	}

	/**
	 * Adds a namespace. Does nothing, if a namespace with the given name
	 * already exists.
	 *
	 * @param name
	 *            The name of the namespace to be added
	 */
	public void addNamespace(String name) {
		// add name to list of namespaces
		jedis.sadd("tdf.namespaces", name);

		// set namespace index counter to -1,
		// so the first used index is 0
		jedis.setnx("tdf." + name + ".index", "-1");

		// Redis does not support empty lists, so the namespace queues are not
		// created here, but implicitly when first used
	}

	/**
	 * Removes a namespace. All tasks in this namespace will be deleted.
	 *
	 * @param name
	 *            The name of the namespace to be removed
	 */
	public void deleteNamespace(String name) {
		// remove name from list of namespaces
		jedis.srem("tdf.namespaces", name);

		// delete namespace index counter
		jedis.del("tdf." + name + ".index");

		// delete all tasks in this namespace
		deleteAllTasks(name);

		// delete namespace lists and sets
		jedis.del("tdf." + name + ".queuing");
		jedis.del("tdf." + name + ".running");
		jedis.del("tdf." + name + ".completed");
		jedis.del("tdf." + name + ".processed");
	}

	/**
	 * Deletes all tasks in the given namespace
	 * 
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of deleted tasks
	 */
	public Long deleteAllTasks(String namespace) {
		Long deleted = 0L;
		for (String taskHashKey : jedis.keys("tdf." + namespace + ".task.*")) {
			Long index = Long.valueOf(taskHashKey.split("\\.")[3]);
			deleteTask(namespace, index);
			deleted++;
		}
		return deleted;
	}

	/**
	 * Increases the index counter for the given namespace by one and returns it
	 *
	 * @param name
	 *            The name of the namespace whose index counter should be
	 *            increased
	 * @return The increased value of the index counter
	 */
	private Long increaseIndexForNamespace(String name) {
		return jedis.incr("tdf." + name + ".index");
	}

	/**
	 * Adds a task to a namespace. If the namespace doesn't exist yet, it will be created.
	 *
	 * @param namespace
	 *            The namespace for the task
	 * @param task
	 *            The task object. At least the worker attribute has to be
	 *            filled.
	 * @return The task's index, if successful, -1 if not
	 */
	public Long addTask(String namespace, Task task) {
		if (task.getWorker().isEmpty()) {
			return -1L;
		}

		// create namespace if it does not exist
		if ( ! jedis.sismember("tdf.namespaces", namespace)) {
			addNamespace(namespace);
		}

		// get index for task
		Long index = increaseIndexForNamespace(namespace);

		// save task with new index == creating a new task
		task.save(jedis, namespace, index);

		// add task to the end of the queuing list
		jedis.rpush("tdf." + namespace + ".queuing", index.toString());

		return index;
	}

	/**
	 * Deletes a task and removes it from the namespace and all lists / sets.
	 * 
	 * @param namespace
	 *            The name of the namespace
	 * @param index
	 *            The index of the task to be removed
	 * @return true, if the task and it's position in one of the queues/sets was
	 *         deleted, false if not
	 */
	public boolean deleteTask(String namespace, Long index) {
		// delete task information
		Long result = jedis.del("tdf." + namespace + ".task." + index);

		// delete task from queuing list and running, completed and processed set
		result += jedis.lrem("tdf." + namespace + ".queuing", 0, index.toString());
		result += jedis.srem("tdf." + namespace + ".running", index.toString());
		result += jedis.srem("tdf." + namespace + ".completed", index.toString());
		result += jedis.srem("tdf." + namespace + ".processed", index.toString());

		return (result == 2);
	}

	/**
	 * Returns a task
	 *
	 * @param namespace
	 *            The namespace of the task
	 * @param index
	 *            The index of the task
	 * @return A task object or null, if the task cannot be found
	 */
	public Task getTask(String namespace, Long index) {
		String hashKey = "tdf." + namespace + ".task." + index;

		if (jedis.hget(hashKey, "worker") == null) {
			return null;
		}

		try {
			return new Task(jedis, namespace, index);
		}
		catch(Exception e) {
			return null;
		}
	}

	/**
	 * Returns a list of queuing task for a namespace
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return A list of tasks
	 */
	public List<Task> getQueuingTasks(String namespace) {
		List<Task> tasks = new ArrayList<Task>();

		for (String index : jedis
				.lrange("tdf." + namespace + ".queuing", 0, -1)) {
			try {
				tasks.add(new Task(jedis, namespace, Long.valueOf(index)));
			}
			catch(Exception e) {
				return null;
			}

		}

		return tasks;
	}

	/**
	 * Deletes all tasks that are currently queuing
	 * 
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of deleted tasks
	 */
	public Long deleteQueuingTasks(String namespace) {
		Long deleted = 0L;
		for (String index : jedis
				.lrange("tdf." + namespace + ".queuing", 0, -1)) {
			deleteTask(namespace, Long.valueOf(index));
			deleted++;
		}
		return deleted;
	}

	/**
	 * Returns the number of currently queuing tasks
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of tasks currently in the queuing list
	 */
	public Long countQueuingTasks(String namespace) {
		return jedis.llen("tdf." + namespace + ".queuing");
	}

	/**
	 * Deletes tasks where the task ID is in one of the queues/sets, but the
	 * task information is not available
	 * 
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of deleted missing tasks
	 */
	public Long deleteMissingTasks(String namespace) {
		Long deleted = 0L;
		for (String index : jedis.lrange("tdf." + namespace + ".queuing", 0, -1)) {
			if (getTask(namespace, Long.valueOf(index)) == null) {
				jedis.lrem("tdf." + namespace + ".queuing", 0, index.toString());
				deleted++;
			}
		}
		for (String index : jedis.smembers("tdf." + namespace + ".running")) {
			if (getTask(namespace, Long.valueOf(index)) == null) {
				jedis.srem("tdf." + namespace + ".running", index.toString());
				deleted++;
			}
		}
		for (String index : jedis.smembers("tdf." + namespace + ".completed")) {
			if (getTask(namespace, Long.valueOf(index)) == null) {
				jedis.srem("tdf." + namespace + ".completed", index.toString());
				deleted++;
			}
		}
		for (String index : jedis.smembers("tdf." + namespace + ".processed")) {
			if (getTask(namespace, Long.valueOf(index)) == null) {
				jedis.srem("tdf." + namespace + ".processed", index.toString());
				deleted++;
			}
		}
		return deleted;
	}

	/**
	 * Returns a set of running tasks for a namespace
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return A set of tasks
	 */
	public Set<Task> getRunningTasks(String namespace) {
		Set<Task> tasks = new HashSet<Task>();

		for (String index : jedis.smembers("tdf." + namespace + ".running")) {

			try {
				tasks.add(new Task(jedis, namespace, Long.valueOf(index)));
			}
			catch(Exception e) {
				return null;
			}

		}

		return tasks;
	}

	/**
	 * Deletes all tasks that are currently running
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of deleted tasks
	 */
	public Long deleteRunningTasks(String namespace) {
		Long deleted = 0L;
		for (String index : jedis.smembers("tdf." + namespace + ".running")) {
			deleteTask(namespace, Long.valueOf(index));
			deleted++;
		}
		return deleted;
	}

	/**
	 * Returns the number of currently running tasks
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of tasks currently in the running set
	 */
	public Long countRunningTasks(String namespace) {
		return jedis.scard("tdf." + namespace + ".running");
	}

	/**
	 * Returns a set of completed tasks for a namespace
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return A set of tasks
	 */
	public Set<Task> getCompletedTasks(String namespace) {
		Set<Task> tasks = new HashSet<Task>();

		for (String index : jedis.smembers("tdf." + namespace + ".completed")) {

			try {
				tasks.add(new Task(jedis, namespace, Long.valueOf(index)));
			}
			catch(Exception e) {
				return null;
			}

		}

		return tasks;
	}

	/**
	 * Deletes all tasks that are completed
	 * 
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of deleted tasks
	 */
	public Long deleteCompletedTasks(String namespace) {
		Long deleted = 0L;
		for (String index : jedis.smembers("tdf." + namespace + ".completed")) {
			deleteTask(namespace, Long.valueOf(index));
			deleted++;
		}
		return deleted;
	}

	/**
	 * Returns the number of completed tasks
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of tasks in the completed set
	 */
	public Long countCompletedTasks(String namespace) {
		return jedis.scard("tdf." + namespace + ".completed");
	}

	/**
	 * Returns a set of processed tasks for a namespace
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return A set of tasks
	 */
	public Set<ClientTask> getProcessedTasks(String namespace) {
		Set<ClientTask> tasks = new HashSet<ClientTask>();

		for (String index : jedis.smembers("tdf." + namespace + ".processed")) {

			try {
				tasks.add(new ClientTask(jedis, namespace, Long.valueOf(index)));
			}
			catch(Exception e) {
				return null;
			}

		}

		return tasks;
	}

	/**
	 * Deletes all tasks that are processed
	 * 
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of deleted tasks
	 */
	public Long deleteProcessedTasks(String namespace) {
		Long deleted = 0L;
		for (String index : jedis.smembers("tdf." + namespace + ".processed")) {
			deleteTask(namespace, Long.valueOf(index));
			deleted++;
		}
		return deleted;
	}

	/**
	 * Returns the number of processed tasks
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return the number of tasks in the processed set
	 */
	public Long countProcessedTasks(String namespace) {
		return jedis.scard("tdf." + namespace + ".processed");
	}

	/**
	 * Moves a task from the "finished" to the "processed" set. If the task is
	 * not in the "finished" set, no operation is performed.
	 *
	 * @param namespace
	 *            The namespace of the task
	 * @param index
	 *            The index of the task
	 */
	public void processTask(String namespace, Long index) {
		jedis.smove("tdf." + namespace + ".completed", "tdf." + namespace + ".processed", index.toString());
	}

	/**
	 * Delete all expired tasks from the running set and waiting queue
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return The number of deleted expired tasks
	 */
	public Long deleteAllExpiredTasks(String namespace) {
		Long deleted = 0L;

		// check the running set
		for (String index : jedis.smembers("tdf." + namespace + ".running")) {
			Task task = getTask(namespace, Long.valueOf(index));

			if (task != null && task.isExpired()) {
				deleted++;
				deleteTask(namespace, Long.valueOf(index));
			}
		}

		// check the waiting queue
		for (String index : jedis
				.lrange("tdf." + namespace + ".queuing", 0, -1)) {
			Task task = getTask(namespace, Long.valueOf(index));

			if (task != null && task.isExpired()) {
				deleted++;
				deleteTask(namespace, Long.valueOf(index));
			}
		}

		return deleted;
	}

	/**
	 * Requeue one specific namespace. All timed out running tasks are put in
	 * front of the queuing list again.
	 *
	 * @param namespace
	 *            The name of the namespace
	 * @return The number of requeued tasks
	 */
	public Long requeue(String namespace) {
		Long requeued = 0L;

		// check the running set
		for (String index : jedis.smembers("tdf." + namespace + ".running")) {
			String hashKey = "tdf." + namespace + ".task." + index;

			Task task = getTask(namespace, Long.valueOf(index));

			if (task != null && task.isTimedOut()) {
				requeued++;

				// remove task from the running set
				jedis.srem("tdf." + namespace + ".running", index);

				// remove client and started information
				jedis.hdel(hashKey, "started");
				jedis.hdel(hashKey, "client");

				// add task to the front of the queuing list
				jedis.lpush("tdf." + namespace + ".queuing", index);
			}
		}

		return requeued;
	}

	/**
	 * Exports processed tasks and deletes them from the Redis database
	 *
	 * @param namespace
	 *            The namespace which should be exported
	 * @param directory
	 *            The directory where the data should be exported
	 * @param input
	 *            If true, the input information will be exported
	 * @param output
	 *            If true, the output information will be exported
	 * @param log
	 *            If true, the log information will be exported
	 * @param error
	 *            If true, the error information will be exported
	 * @param information
	 *            If true, all other task information fields will be exported
	 * @throws IOException
	 *             Thrown if something went wrong while writing information to
	 *             file
	 * @return The number of exported tasks
	 */
	public Long exportProcessedTasks(String namespace, File directory,
			boolean input, boolean output, boolean log, boolean error,
			boolean information) throws IOException {
		Long exported = 0L;

		for (ClientTask task : getProcessedTasks(namespace)) {
			exported++;

			task.setBaseDir(directory);
			task.getTaskIndexDir().mkdirs();

			// export requested task information
			if (input) {
				FileUtils.writeStringToFile(task.getInputFile(), task.getInput());
			}
			if (output) {
				FileUtils.writeStringToFile(task.getOutputFile(), task.getOutput());
			}
			if (log) {
				FileUtils.writeStringToFile(task.getLogFile(), task.getLog());
			}
			if (error) {
				FileUtils.writeStringToFile(task.getErrorFile(), task.getError());
			}
			if (information) {
				FileUtils.writeStringToFile(new File(task.getTaskIndexDir(), "information.txt"), task.asString());
			}

			// delete the task
			deleteTask(namespace, task.getIndex());
		}

		return exported;
	}

	/**
	 * Closes the connection to the redis database
	 */
	public void close() {
		jedis.disconnect();
	}

}
