package de.tuda.p2p.tdf.client;

import java.io.File;
import java.util.List;

import org.joda.time.DateTime;

import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.Logger;

public class TaskExecutor extends Thread {

	private Integer waitQueueEmpty;
	private Integer waitQueueExpired;
	private Integer waitAfterSuccess;
	private Integer waitAfterSetupError;
	private Integer waitAfterRunError;
	private Integer shutdown;
	private TaskInterfaceClient taskInterface;
	private File workingDir;
	private String clientId;

	/**
	 * Constructor
	 * 
	 * @param waitQueueEmpty
	 *            The time in milliseconds to wait before trying to get a new
	 *            task when the queue is empty
	 * @param waitQueueExpired
	 *            The time in milliseconds to wait before trying to get a new
	 *            task when the queue has only expired tasks
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
	 * @param workingDir
	 *            The client working directory
	 * @param namespaces
	 *            The namespaces to execute
	 */
	public TaskExecutor(Integer waitQueueEmpty, Integer waitQueueExpired, Integer waitAfterSuccess,
			Integer waitAfterSetupError, Integer waitAfterRunError, Integer shutdown,
			String clientId, String host, Integer port, Integer index,
			String auth, File workingDir, List<String> namespaces) {
		this.waitQueueEmpty = waitQueueEmpty;
		this.waitQueueExpired = waitQueueExpired;
		this.waitAfterSuccess = waitAfterSuccess;
		this.waitAfterSetupError = waitAfterSetupError;
		this.waitAfterRunError = waitAfterRunError;
		this.shutdown = shutdown;
		this.workingDir = workingDir;
		this.taskInterface = new TaskInterfaceClient(clientId, host, port,
				index, auth, namespaces);
		this.clientId=clientId;

		if ( ! workingDir.exists()) {
			Client.logMessage("Directory '" + workingDir.getAbsolutePath() + "' does not exist, create it.", true);
			workingDir.mkdirs();
		}

		Client.logMessage("Start...");
		Logger.debug("Client_Init,"+clientId);
	}

	/**
	 * The run loop
	 */
	@Override
	public void run() {
		DateTime start = DateTime.now();
		while (true) {
			try {
				ClientTask task = taskInterface.getTaskToExecute(waitQueueExpired);
				if (task == null) {
					Client.logMessage("Wait " + waitQueueEmpty + "ms for new tasks.", true);

					// no tasks currently queuing, wait some time
					sleep(waitQueueEmpty);
				} else {
					try {
						Client.logMessage("Execute task '" + task.toString() + "'");
						Logger.debug("Task_Init,"+clientId+","+task.getNamespace()+"."+task.getIndex());

						// execute task
						task.setBaseDir(workingDir);
						taskInterface.prepareWorker(task);

						if (taskInterface.runSetupScript(task)) {
							taskInterface.runTask(task);
						} else {
							// wait some time
							waitSomeTime(task.getWaitAfterSetupError(), waitAfterSetupError);

							throw new TaskException("Setup script did not exit with 0.");
						}
						
						Logger.debug("Task_Success,"+clientId+","+task.getNamespace()+"."+task.getIndex());
						
						// wait some time
						waitSomeTime(task.getWaitAfterSuccess(), waitAfterSuccess);
					} catch (TaskException e) {
						taskInterface.fail(task, e.getMessage());

						// wait some time
						waitSomeTime(task.getWaitAfterRunError(), waitAfterRunError);
					} catch (SetupException e) {
						taskInterface.fail(task, e.getMessage());

						// wait some time
						waitSomeTime(task.getWaitAfterSetupError(), waitAfterSetupError);
					}
				}
			} catch (InterruptedException e) {
				Client.logError("Error while waiting for task: " + e.getMessage());
				break;
			}

			// check if the client should be shut down
			if (shutdown != null) {
				if (start.plusMillis(shutdown).isBeforeNow()) {
					Client.logMessage("Shutting down.");
					Logger.debug("Client_shutdown,"+clientId);
					break;
				}
			}
		}
	}

	/**
	 * Wait for the maximum of both times
	 * 
	 * @param waitTime1
	 *            the time to wait in milliseconds
	 * @param waitTime2
	 *            the time to wait in milliseconds
	 * @throws InterruptedException
	 *             thrown if the process was interrupted while waiting
	 */
	private void waitSomeTime(Integer waitTime1,
			Integer waitTime2) throws InterruptedException {
		if (waitTime1 == null) {
			if (waitTime2 == null) {
				return;
			}
			Client.logMessage("Wait " + waitTime2 + "ms.", true);
			sleep(waitTime2);
		} else {
			if (waitTime2 == null) {
				Client.logMessage("Wait " + waitTime1 + "ms.", true);
				sleep(waitTime1);
				return;
			}
			Client.logMessage("Wait " + Math.max(waitTime1, waitTime2) + "ms.", true);
			sleep(Math.max(waitTime1, waitTime2));
		}
	}

}
