package de.tuda.p2p.tdf.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.Logger;
import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.common.TaskList;

public class TaskInterfaceClient {

	private String host;
	private Integer port;
	private Integer index;
	private String auth;

	private String clientId;
	private Jedis jedis;
	private List<String> namespaces;
	private TaskList taskList;

	/**
	 * Constructor
	 *
	 * @param clientId
	 *            The id to identify the client
	 * @param host
	 *            The Redis server host
	 * @param port
	 *            The Redis server port
	 * @param index
	 *            The Redis database index
	 * @param auth
	 *            The Redis password
	 * @param namespaces
	 *            The namespaces to execute
	 */
	public TaskInterfaceClient(String clientId, String host, Integer port,
			Integer index, String auth, List<String> namespaces) {
		this.clientId = clientId;
		this.namespaces = namespaces;

		this.host = host;
		this.port = port;
		this.index = index;
		this.auth = auth;
		
		connect();
	}

	/**
	 * Connect to the Redis database
	 */
	private void connect() {
		this.jedis = new Jedis(host, port);
		if (auth != null && !auth.isEmpty()) {
			jedis.auth(auth);
		}
		jedis.select(index);			
	}		

	/**
	 * Gives access to the Redis library directly
	 *
	 * @return The Jedis object
	 */
	public Jedis getJedis() {
		Integer i = 0;
		while ( ! jedis.isConnected() || i < 10) {
			i++;
			try {
				jedis.ping();
			} catch (JedisConnectionException e) {
				Client.logError("Connection lost, try to reconnect.", true);
				try {
					connect();
				} catch (JedisConnectionException ex) {
					// Try to connect 10 times, wait 1s between, then give up					
					if ( i >= 10) {
						throw e;
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						throw e;
					}					
				}
			}
		}

		return jedis;			
	}

	/**
	 * Returns the next task to execute from any namespace
	 * 
	 * @param waitQueueExpired
	 *            the time in milliseconds to wait if the queue has only expired
	 *            tasks
	 * @return A task object or null, if no tasks are currently queuing
	 * @throws InterruptedException
	 *             Thrown if the thread is interrupted while waiting for the
	 *             task to be valid
	 */
	public ClientTask getTaskToExecute(Integer waitQueueExpired) throws InterruptedException {
		//just to be sure... (we shuffle this later and don't want this to interfere with other functions)
		
		if (namespaces == null || namespaces.isEmpty()) {
			// execute tasks from all namespaces
			Client.logMessage("Execute tasks from all namespaces", true);
			namespaces = new ArrayList<String>(getJedis().smembers("tdf.namespaces"));
		}
		List<String> namespaces=new ArrayList<String>(this.namespaces);
		if (getTaskList() == null || getTaskList().getOpenTasks().isEmpty()){
			Collections.shuffle(namespaces);
		for (String namespace : namespaces) {
			if (getTaskList() == null || getTaskList().getOpenTasks().isEmpty()) {
				setTaskList(getTaskListToExecute(namespace, waitQueueExpired));
			}else break;

		}}
		if (getTaskList() == null) return null;
		Task t =getTaskList().getOpenTasks().getany();
		try {
			ClientTask ct = new ClientTask(t);
			jedis.lrem("tdf."+ct.getNamespace()+".queuing", 1, ct.getIndex().toString());
			jedis.sadd("tdf."+ct.getNamespace()+".running", ct.getIndex().toString());
			return ct;
		} catch (FileNotFoundException e) {
			getTaskList().deltask(t);
			
			return null;
		}
		
	}

	private TaskList getTaskList() {
		// TODO Auto-generated method stub
		return this.taskList;
	}

	private void setTaskList(TaskList taskList) {
		this.taskList = taskList;
	}

	/**
	 * Returns the next task to execute from a given namespace
	 *
	 * @param namespace
	 *            The namespace of the task
	 * @param waitQueueExpired
	 *            The time to wait in milliseconds when the queue has only expired tasks
	 * @return A task object or null, if no tasks are currently queuing in this
	 *         namespace
	 * @throws InterruptedException
	 *             Thrown if the thread is interrupted while waiting for the
	 *             task to be valid
	 */
	@SuppressWarnings("unused")
	private ClientTask getTaskToExecute(String namespace, Integer waitQueueExpired) throws InterruptedException {
		String index;
		ClientTask task;

		Client.logMessage("Get task from namespace: " + namespace, true);
		Long firstExpiredIndex = null;
		do {
			index = getJedis().lpop("tdf." + namespace + ".queuing");
			if (index == null) {
				Client.logMessage("Queuing list is empty", true);
				return null;
			}

			task = getTask(namespace, Long.valueOf(index));
			if (task == null) {
				Client.logError("Task '" + index + "' returned null.", true);
				return null;
			}

			if (task.isExpired()) {
				Client.logMessage("Task '" + index + "' is expired.", true);

				// add the expired task to the end of the queue, the server
				// needs to delete them
				getJedis().rpush("tdf." + namespace + ".queuing", index);				

				if (firstExpiredIndex == null) {
					firstExpiredIndex = task.getIndex();					
				} else {
					if (firstExpiredIndex == task.getIndex()) {
						Client.logMessage("All tasks are expired, wait " + waitQueueExpired + "ms.", true);
						Thread.sleep(waitQueueExpired);
						firstExpiredIndex = null;
					}
				}
			}
		} while (task.isExpired());

		getJedis().sadd("tdf." + namespace + ".running", index);

		if ( ! task.isValid()) {
			// wait for the task to be valid
			Client.logMessage("Waiting for the task '" + index + "' to get valid...");
			Thread.sleep(task.validWaitTime());
		}

		return task;
	}
	private TaskList getTaskListToExecute(String namespace, Integer waitQueueExpired) throws InterruptedException {
		String index;
		TaskList tasklist;

		Client.logMessage("Get tasklist from namespace: " + namespace, true);
		Long firstExpiredIndex = null;
		do {
			index = getJedis().lpop("tdf." + namespace + ".queuinglists");
			if (index == null) {
				Client.logMessage("Queuing list is empty", true);
				return null;
			}
			tasklist = new TaskList();
			try {
				tasklist .load(getJedis(), namespace, Long.valueOf(index));
			} catch ( FileNotFoundException e) {
				Client.logError("Tasklist '" + index + "' returned null.", true);
				return null;
			}
			if (tasklist.isExpired()) {
				Client.logMessage("Tasklist '" + index + "' is expired.", true);

				// add the expired task to the end of the queue, the server
				// needs to delete them
				getJedis().rpush("tdf." + namespace + ".queuing", index);				

				if (firstExpiredIndex == null) {
					firstExpiredIndex = tasklist.getIndex();					
				} else {
					if (firstExpiredIndex == tasklist.getIndex()) {
						Client.logMessage("All tasks are expired, wait " + waitQueueExpired + "ms.", true);
						Thread.sleep(waitQueueExpired);
						firstExpiredIndex = null;
					}
				}
			}
		} while (tasklist.isExpired());

		getJedis().sadd("tdf." + namespace + ".running", index);

		if ( ! tasklist.isValid()) {
			// wait for the task to be valid
			Client.logMessage("Waiting for the task '" + index + "' to get valid...");
			Thread.sleep(tasklist.validWaitTime());
		}
		String Tasks ="";
		for (Task task : tasklist.getTasks())
			Tasks+=","+task.getNamespace()+"."+task.getIndex();
		Logger.debug("Claim_Tasklist,"+clientId+","+tasklist.getNamespace()+"."+tasklist.getIndex()+Tasks);
		
		
		return tasklist;
	}

	/**
	 * Returns a task object
	 *
	 * @param namespace
	 *            The namespace of the task
	 * @param index
	 *            The index of the task
	 * @return A task object or null, if the task cannot be found
	 */
	private ClientTask getTask(String namespace, Long index) {
		String hashKey = "tdf." + namespace + ".task." + index;

		if (getJedis().hget(hashKey, "worker") == null) {
			Client.logError("Task '" + index + "' found, but worker information empty.", true);
			return null;
		}

		try {
			return new ClientTask(getJedis(), namespace, index);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Prepare the worker to execute the task. Can download new or update
	 * existing workers.
	 *
	 * @param task
	 *            The task to be executed
	 * @throws SetupException
	 *             Thrown if something went wrong and the worker preparation failed.
	 */
	public void prepareWorker(ClientTask task) throws SetupException {
		URI workerUri = task.getWorkerUri();
		if (workerUri == null) {
			throw new SetupException("Worker information cannot be parsed.");
		}

		Client.logMessage("Create task namespace dir: " + task.getNamespaceDir().getAbsolutePath(), true);
		task.getNamespaceDir().mkdirs();

		if (workerUri.getScheme().equals("http") || workerUri.getScheme().equals("https") || workerUri.getScheme().equals("file")) {
			prepareHttpWorker(task);
		} else {
			throw new SetupException("Worker format '"
					+ task.getWorkerUri().toString() + "' is not supported.");
		}

		task.getTempDir().mkdirs();
		task.getTasksDir().mkdirs();
		task.getSessionDir().mkdirs();
	}

	/**
	 * Executes the worker's setup script.
	 *
	 * @param task
	 *            The task object
	 * @return true, if the setup script returned 0 (and everything is ok),
	 *         false if not
	 * @throws SetupException
	 *             Thrown if something went wrong during the worker setup
	 */
	public boolean runSetupScript(ClientTask task) throws SetupException {
		prepareTask(task);
		Client.logMessage("Run setup script.", true);
		try {
			return executeScript(task, "setup.sh");
		} catch (TaskException e) {
			throw new SetupException(e.getMessage());
		}
	}

	/**
	 * Runs chmod u+x on the file to make it executable
	 *
	 * @param file
	 *            The file (script) to be changed
	 * @throws TaskException
	 *             Thrown if the permissions couldn't be set
	 */
	private void setScriptExecutable(File file) throws TaskException {
		try {
			Client.logMessage("Make " + file.getAbsolutePath() + " executable.", true);
			String cmd[] = { "chmod", "u+x", file.getAbsolutePath() };
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
		} catch (IOException e) {
			throw new TaskException(
					"Error while setting permissions for script: "
							+ e.getMessage());
		} catch (InterruptedException e) {
			throw new TaskException(
					"Setting permissions for script was interrupted: "
							+ e.getMessage());
		}
	}

	/**
	 * Downloads a worker from http
	 *
	 * @param task
	 *            The task to be executed
	 * @throws SetupException
	 *             Thrown if something went wrong and the task is failed
	 */
	private void prepareHttpWorker(ClientTask task) throws SetupException {
		if (task.getWorkerDir().exists()) {
			Client.logMessage("Task worker exists, no need to download it again.", true);
			return;
		}

		try {
			// download file
			File workerFile = new File(task.getNamespaceDir(), task.getWorkerFileName());
			Client.logMessage("Download worker from: " + task.getWorkerUri().toString() + " to: " + workerFile.getAbsolutePath(), true);
			FileUtils.copyURLToFile(task.getWorkerUri().toURL(), workerFile);

			// unpack file
			Client.logMessage("Unzip worker archive.", true);
			ZipFile workerArchive = new ZipFile(workerFile);

			workerArchive.extractAll(task.getBinDir().getCanonicalPath());

			// delete file
			Client.logMessage("Delete worker archive.", true);
			workerFile.delete();

			return;
		} catch (MalformedURLException e) {
			throw new SetupException("Error in worker URL: " + e.getMessage());
		} catch (IOException e) {
			throw new SetupException("Error while downloading worker: " + e.getMessage());
		} catch (ZipException e) {
			throw new SetupException("Error while unzipping worker: " + e.getMessage());
		}
	}

	/**
	 * The task execution has failed, add an error message and move it to the
	 * finished set
	 *
	 * @param errorMessage
	 *            The error message
	 */
	public void fail(ClientTask task, String errorMessage) {
		Client.logError("Task '" + task.getNamespace() + "'.'"
				+ task.getIndex() + "' error: " + errorMessage);
		Logger.error("Task_Fail,"+clientId+","+task.getNamespace() + "."
				+ task.getIndex());
		task.setError(errorMessage);
		task.setFinished(DateTime.now());
		

		if (task.save(jedis) == -1L) {
			Client.logError("Task '"
							+ task.getNamespace()
							+ "'.'"
							+ task.getIndex()
							+ "' failed, saving back to Redis failed also. Original error message: "
							+ errorMessage);
		}
	}

	/**
	 * Runs the task
	 *
	 * @param task
	 *            The task object
	 * @param baseDir
	 *            The worker directory
	 * @throws TaskException
	 *             Thrown if something went wrong while executing the task
	 * @return The task object with filled output, log and error fields
	 */
	public ClientTask runTask(ClientTask task) throws TaskException {
		Logger.log("Task_Start,"+clientId+","+task.getNamespace()+"."+task.getIndex());
		if (!executeScript(task, "run.sh"))
			throw new TaskException("run.sh did not return 0\nhave this stderr:\n\n"+emergencyReadErrorFile(task));
		
		
		ClientTask t = getTask(task.getNamespace(), task.getIndex());
		if (t != null && t.getClient().equals(clientId)) {
			Client.logMessage("Writing output, log and error from files to Redis.", true);
			// set output, log and error capture
			task.setOutput(readFile(task.getOutputFile()));
			task.setLog(readFile(task.getLogFile()));
			task.setError(readFile(task.getErrorFile()));

			// set finished timestamp
			task.setFinished(DateTime.now());
			task.save(jedis);

			// move to finished set
			Client.logMessage("Move task to completed set.", true);
			jedis.smove("tdf." + task.getNamespace() + ".running",
						"tdf." + task.getNamespace() + ".completed",
						task.getIndex().toString());
		} else {
			throw new TaskException("Task deleted in redis or client id does not match. Will not save task results to redis.");
		}

		return task;
	}

	private String emergencyReadErrorFile(ClientTask task) {
		String s;
		try {
			s=readFile(task.getErrorFile());
		} catch (TaskException e) {
			s="failed to get stderr";
		}
		return s;
	}



	/**
	 * Reads a file and return the content as a String
	 *
	 * @param file
	 *            The file to be read
	 * @return The file's content
	 * @throws TaskException
	 *             Thrown if something went wrong while reading the file
	 */
	private String readFile(File file) throws TaskException {
		try {
			if (file.exists()) {
				Client.logMessage("Read file: " + file.getAbsolutePath(), true);
				return FileUtils.readFileToString(file);
			}
			return "";
		} catch (IOException e) {
			throw new TaskException("Error while reading file: "
					+ e.getMessage());
		}
	}

	/**
	 * Executes a script (setup.sh or run.sh), captures STDOUT and STDERR and
	 * writes them to files
	 *
	 * @param task
	 *            The task object
	 * @param script
	 *            the script to execute (run.sh or setup.sh)
	 * @return true, if the script returned 0 (and everything is ok), false if
	 *         not
	 * @throws TaskException
	 *             Thrown if an error occurred while executing the task
	 */
	private boolean executeScript(ClientTask task, String script) throws TaskException {
		setScriptExecutable(new File(task.getBinDir(), script));

		Timer timer = null;
		Process p = null;

		try {
			ProcessBuilder pb = new ProcessBuilder("./" + script, task
					.getInputFile().getCanonicalPath(), task.getOutputFile()
					.getCanonicalPath(), task.getTempDir().getCanonicalPath());
			pb.directory(task.getBinDir());

			// timer for timeout
			if (task.getTimeout() != null) {
				timer = new Timer(true);
				InterruptTimerTask interrupter = new InterruptTimerTask(Thread.currentThread());
				timer.schedule(interrupter, task.getTimeout());				
			}

			Client.logMessage("Start " + pb.command() + " with timeout " + task.getTimeout(), true);

			// start worker
			p = pb.start();
			StreamGobbler captureOut = new StreamGobbler(p.getInputStream(),
					task.getLogFile());
			StreamGobbler captureErr = new StreamGobbler(p.getErrorStream(),
					task.getErrorFile());

			// capture STDOUT and STDIN
			captureOut.start();
			captureErr.start();

			// wait for worker to end
			Client.logMessage("Wait for the script to finish.", true);
			p.waitFor();

			// wait for capture finish
			captureOut.join();
			captureErr.join();

			return (p.exitValue() == 0);
		} catch (IOException e) {
			throw new TaskException("Script failed: " + e.getMessage());
		} catch (InterruptedException e) {
			throw new TaskException("Worker timeout, force quit.");
		} finally {
			if (p != null) {
				p.destroy();
			}
			if (timer != null) {
				timer.cancel();
			}
			Thread.interrupted();
		}
	}

	/**
	 * Just a simple TimerTask that interrupts the specified thread when run.
	 */
	class InterruptTimerTask extends TimerTask {
		private Thread thread;

		public InterruptTimerTask(Thread t) {
			this.thread = t;
		}

		public void run() {
			thread.interrupt();
		}
	}

	/**
	 * Create task directory and write input to file
	 *
	 * @param task
	 *            The task object
	 * @throws SetupException
	 *             Thrown if something went wrong while preparing the task files
	 */
	private void prepareTask(ClientTask task) throws SetupException {
		// create task index directory
		File taskIndexDir = task.getTaskIndexDir();
		Integer i = 0;
		while (taskIndexDir.exists()) {
			i++;
			taskIndexDir = new File(task.getSessionDir(), task.getIndex().toString() + "-" + i);
		}
		Client.logMessage("Use task index directory: " + taskIndexDir.getAbsolutePath(), true);
		task.setTaskIndexDir(taskIndexDir);
		taskIndexDir.mkdirs();

		// create (empty) output file
		try {
			Client.logMessage("Create empty output file.", true);
			FileUtils.touch(task.getOutputFile());
		} catch (IOException e) {
			throw new SetupException("Error while creating output file: " + e.getMessage());
		}

		// write input in file
		try {
			Client.logMessage("Write input to file.", true);
			FileUtils.writeStringToFile(task.getInputFile(), task.getInput());
		} catch (IOException e) {
			throw new SetupException("Error while creating input file: " + e.getMessage());
		}
	}

}
