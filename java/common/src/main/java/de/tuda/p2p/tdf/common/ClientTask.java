package de.tuda.p2p.tdf.common;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FilenameUtils;
import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;

public class ClientTask extends Task {

	// the base working directory
	File baseDir;

	File taskIndexDir;

	/**
	 * Constructor that fills itself from Redis
	 * 
	 * @param jedis
	 *            The Jedis instance
	 * @param hashKey
	 *            The task information hash key
	 */
	public ClientTask(Jedis jedis, String namespace, Long index) {
		super(jedis, namespace, index);
	}

	public ClientTask() {
	}

	/**
	 * Returns ${namespace}.${index}
	 */
	@Override
	public String toString() {
		if (getNamespace() == null) {
			if (getIndex() == null) {
				return super.toString();
			}
			return getIndex().toString();
		}
		return getNamespace() + "." + getIndex();
	}

	public File getBaseDir() {
		return baseDir;
	}

	public void setBaseDir(File baseDir) {
		this.baseDir = baseDir;
	}

	/**
	 * Returns the namespace directory for this task
	 * 
	 * @return ${base-dir}/${namespace} or null, if getBaseDir() returns null or
	 *         getNamespace() returns null or is empty
	 */
	public File getNamespaceDir() {
		if (getBaseDir() == null || getNamespace() == null || getNamespace().isEmpty()) {
			return null;
		}
		return new File(getBaseDir(), getNamespace());
	}

	/**
	 * Returns the worker directory
	 * 
	 * @return ${namespace-dir}/${worker-name} or null, if getNamespaceDir() or
	 *         getWorkerName() returns null
	 */
	public File getWorkerDir() {
		if (getNamespaceDir() == null || getWorkerName() == null) {
			return null;
		}
		return new File(getNamespaceDir(), getWorkerName());
	}

	/**
	 * Returns the directory for the worker's code
	 * 
	 * @return ${worker-dir}/bin or null, if getWorkerDir() returns null
	 */
	public File getBinDir() {
		if (getWorkerDir() == null) {
			return null;
		}
		return new File(getWorkerDir(), "bin");
	}

	/**
	 * Returns the temp directory, that workers can use
	 * 
	 * @return ${worker-dir}/temp or null, if getWorkerDir() returns null
	 */
	public File getTempDir() {
		if (getWorkerDir() == null) {
			return null;
		}
		return new File(getWorkerDir(), "temp");
	}

	/**
	 * Returns the parent directory of all tasks for this worker
	 * 
	 * @return ${worker-dir}/tasks or null, if getWorkerDir() returns null
	 */
	public File getTasksDir() {
		if (getWorkerDir() == null) {
			return null;
		}
		return new File(getWorkerDir(), "tasks");
	}

	/**
	 * Returns the session directory for this task
	 * 
	 * @return ${tasks-dir}/${session} or null, if getTasksDir() returns null or
	 *         getSession() returns null or is empty
	 */
	public File getSessionDir() {
		if (getTasksDir() == null || getSession() == null || getSession().isEmpty()) {
			return null;
		}
		return new File(getTasksDir(), getSession());
	}

	/**
	 * Returns the task's directory
	 * 
	 * @return ${session-dir}/${task-index} or null, if getSessionDir() or
	 *         getIndex() returns null
	 */
	public File getTaskIndexDir() {
		if (getSessionDir() == null || getIndex() == null) {
			return null;
		}
		if (taskIndexDir == null) {
			return new File(getSessionDir(), getIndex().toString());
		}
		return taskIndexDir;
	}

	/**
	 * Set the task index dir manually. Used when the directory already exists.
	 * 
	 * @param taskIndexDir
	 *            The task index dir
	 */
	public void setTaskIndexDir(File taskIndexDir) {
		this.taskIndexDir = taskIndexDir;
	}

	/**
	 * Returns the output file
	 * 
	 * @return ${task-index-dir}/output.txt or null, if getTaskIndexDir()
	 *         returns null
	 */
	public File getOutputFile() {
		if (getTaskIndexDir() == null) {
			return null;
		}
		return new File(getTaskIndexDir(), "output.txt");
	}

	/**
	 * Returns the input file
	 * 
	 * @return ${task-index-dir}/input.txt or null, if getTaskIndexDir()
	 *         returns null
	 */
	public File getInputFile() {
		if (getTaskIndexDir() == null) {
			return null;
		}
		return new File(getTaskIndexDir(), "input.txt");
	}

	/**
	 * Returns the log file
	 * 
	 * @return ${task-index-dir}/log.txt or null, if getTaskIndexDir()
	 *         returns null
	 */
	public File getLogFile() {
		if (getTaskIndexDir() == null) {
			return null;
		}
		return new File(getTaskIndexDir(), "log.txt");
	}

	/**
	 * Returns the error file
	 * 
	 * @return ${task-index-dir}/error.txt or null, if getTaskIndexDir()
	 *         returns null
	 */
	public File getErrorFile() {
		if (getTaskIndexDir() == null) {
			return null;
		}
		return new File(getTaskIndexDir(), "error.txt");
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
	@Override
	public Long save(Jedis jedis, String namespace, Long index) {
		Long result = super.save(jedis, namespace, index);

		if (result == -1L) {
			return -1L;
		}

		String hashKey = "tdf." + namespace + ".task." + index;

		if (!getOutput().isEmpty()) {
			jedis.hset(hashKey, "output", getOutput());
		}
		if (!getLog().isEmpty()) {
			jedis.hset(hashKey, "log", getLog());
		}
		if (!getError().isEmpty()) {
			jedis.hset(hashKey, "error", getError());
		}
		if (!getClient().isEmpty()) {
			jedis.hset(hashKey, "client", getClient());
		}
		if (getStartedAsString() != null) {
			jedis.hset(hashKey, "started", getStartedAsString());
		}
		if (getFinishedAsString() != null) {
			jedis.hset(hashKey, "finished", getFinishedAsString());
		}

		return index;
	}

	public void setOutput(String output) {
		if (output != null) {
			super.setOutput(output);
		} else {
			super.setOutput("");
		}
	}

	public void setLog(String log) {
		if (log != null) {
			super.setLog(log);
		} else {
			super.setLog("");
		}
	}

	public void setError(String error) {
		if (error != null) {
			super.setError(error);
		} else {
			super.setError("");
		}
	}

	public void setClient(String client) {
		if (client != null) {
			super.setClient(client);
		} else {
			super.setClient("");
		}
	}

	public void setStarted(DateTime started) {
		super.setStarted(started);
	}

	public void setFinished(DateTime finished) {
		super.setFinished(finished);
	}

	/**
	 * @return the worker's URI or null, if it can't be parsed
	 */
	public URI getWorkerUri() {
		try {
			return new URI(getWorker());
		} catch (URISyntaxException e) {
			return null;
		}
	}

	/**
	 * @return the worker's file name or null, if it can't be parsed
	 */
	public String getWorkerFileName() {
		if (getWorkerUri() == null) {
			return null;
		}
		File workerFile = new File(getWorkerUri().getPath());
		return workerFile.getName();
	}

	/**
	 * @return the worker's name (that is: the filename without the extension),
	 *         the task's worker information, if the filename can't be parsed or
	 *         null, if the task's worker information is missing
	 */
	public String getWorkerName() {
		if (getWorkerFileName() == null) {
			if (getWorker() == null || getWorker().isEmpty()) {
				return null;
			}
			return getWorker();
		}
		return FilenameUtils.removeExtension(getWorkerFileName());
	}

}
