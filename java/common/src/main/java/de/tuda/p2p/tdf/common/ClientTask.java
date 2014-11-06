package de.tuda.p2p.tdf.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FilenameUtils;
import org.joda.time.DateTime;

import de.tuda.p2p.tdf.common.databaseObjects.Task;

import redis.clients.jedis.Jedis;

public class ClientTask extends Task {

	// the base working directory
	File baseDir;

	File taskIndexDir;

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

	private String getSession() {
		return (String) this.getField("session");
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

	public Object getIndex() {
		String[] dbKeySplit = this.getDbKey().split(":");
		return dbKeySplit[dbKeySplit.length-1];
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
		this.saveToDB(jedis, this.getDbKey());
		return (Long) getIndex();
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
		return this.save(jedis);
	}

	public void setOutput(String output) {
		this.setField("output", output);
	}

	public void setLog(String log) {
		this.setField("log", log);
	}

	public void setError(String error) {
		this.setField("error", error);
	}

	public void setClient(String client) {
		this.setField("client", client);
	}

	public void setStarted(DateTime started) {
		this.setField("started", started);
	}

	public void setFinished(DateTime finished) {
		this.setField("finished", finished);
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

	private String getWorker() {
		return (String) this.getField("worker");
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

	public String getInput() {
		return (String) this.getField("input");
	}

	public Long getTimeout() {
		return (Long) this.getField("timeout");
	}

	public String getClient() {
		return (String) this.getField("client");
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

	public Integer getWaitAfterSetupError() {
		return (Integer) this.getField("waitAfterSetupError");
	}
	
	public Integer getWaitAfterSuccess() {
		return (Integer) this.getField("waitAfterSuccess");
	}

	public Integer getWaitAfterRunError() {
		return (Integer) this.getField("waitAfterRunError");
	}

}
