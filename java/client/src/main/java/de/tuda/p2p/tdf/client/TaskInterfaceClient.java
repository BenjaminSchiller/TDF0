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
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;

public class TaskInterfaceClient {

	private String host;
	private Integer port;
	private Integer index;
	private String auth;

	private String clientId;
	protected List<String> namespaces;
	private DatabaseFactory dbFactory;


	public TaskInterfaceClient(String clientId, DatabaseFactory dbFactory, List<String> namespaces) {
		this.clientId = clientId;
		this.namespaces = namespaces;

		this.dbFactory = dbFactory;
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
		Client.logError("Task '" + task.getNamespace() + "':'"
				+ task.getIndex() + "' error: " + errorMessage);
		//Logger.error("Task_Fail,"+clientId+","+task.getNamespace() + ":"
		//		+ task.getIndex());
		task.setError(errorMessage);
		task.setFinished(DateTime.now());
		
		dbFactory.addTaskToFailed(task);

		dbFactory.saveTask(task);
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
		//Logger.log("Task_Start,"+clientId+","+task.getNamespace()+":"+task.getIndex());
		if (!executeScript(task, "run.sh"))
			throw new TaskException("run.sh did not return 0\nhave this stderr:\n\n"+emergencyReadErrorFile(task));
		
		
		ClientTask t = task;
		if (t != null && t.getClient().equals(clientId)) {
			Client.logMessage("Writing output, log and error from files to Redis.", true);
			// set output, log and error capture
			task.setOutput(readFile(task.getOutputFile()));
			task.setLog(readFile(task.getLogFile()));
			task.setError(readFile(task.getErrorFile()));

			// set finished timestamp
			task.setFinished(DateTime.now());
			dbFactory.saveTask(task);

			// move to finished set
			Client.logMessage("Move task to completed set.", true);

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
			
			Long tm = task.getTimeout(); 

			// timer for timeout
			if (tm != null) {
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
			throw new TaskTimeoutException("Worker timeout, force quit.");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally {
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
