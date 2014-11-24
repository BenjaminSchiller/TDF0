package de.tuda.p2p.tdf.client;

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.Logger;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;

public class TaskExecutor extends Thread {

	private Integer waitQueueEmpty;
	private Integer waitQueueExpired;
	private Integer waitAfterSuccess;
	private Integer waitAfterSetupError;
	private Integer waitAfterRunError;
	private Integer shutdown;
	private DatabaseFactory dbFactory;
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
		this.dbFactory = new DatabaseFactory(host, port.toString(), index.toString(), auth);
		this.taskInterface = new TaskInterfaceClient(clientId, dbFactory, namespaces);
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
			//FIXME: if namespaces given, only from those
			TaskList taskList = null;
			if(taskInterface.namespaces.isEmpty()) {
				Client.logMessage("Waiting on tasks");
				taskList = dbFactory.getOpenTaskList();
			}
			else {
				Client.logMessage("Waiting on tasks from namespaces " + taskInterface.namespaces);
				taskList = dbFactory.getOpenTaskList(taskInterface.namespaces);
			}
			
			for(ClientTask clTask : taskList.getClientTasks()){
						
				System.out.println("b");
				
				clTask.start(clientId);
				System.out.println("c");

				Client.logMessage("Execute task '" + clTask.toString() + "'");
				Logger.debug("Task_Init,"+clientId+","+clTask.getNamespace()+":"+clTask.getIndex());

				// execute task	
				try {
					clTask.setBaseDir(workingDir);
					taskInterface.prepareWorker(clTask);
	
					if (taskInterface.runSetupScript(clTask)) {
						taskInterface.runTask(clTask);
					} else {
						// wait some time
						waitSomeTime(clTask.getWaitAfterSetupError(), waitAfterSetupError);
	
						throw new TaskException("Setup script did not exit with 0.");
					}
					
					Logger.debug("Task_Success,"+clientId+","+clTask.getNamespace()+":"+clTask.getIndex());
					
					// wait some time
					waitSomeTime(clTask.getWaitAfterSuccess(), waitAfterSuccess);
				}
				catch (TaskException e) {
					taskInterface.fail(clTask, e.getMessage());

					// wait some time
					waitSomeTime(clTask.getWaitAfterRunError(), waitAfterRunError);
				} catch (SetupException e) {
					taskInterface.fail(clTask, e.getMessage());

					// wait some time
					waitSomeTime(clTask.getWaitAfterSetupError(), waitAfterSetupError);
				}
				dbFactory.addProcessedTask(clTask);
			}
			
		}
	}

	
	/**
	 * The old run loop
	 *
	 */
//	public void run() {
//		DateTime start = DateTime.now();
//		while (true) {
//			try {
//				ClientTask task = taskInterface.getTaskToExecute(waitQueueExpired);
//				if (task == null) {
//					Client.logMessage("Wait " + waitQueueEmpty + "ms for new tasks.", true);
//
//					// no tasks currently queuing, wait some time
//					sleep(waitQueueEmpty);
//				} else {
//					try {
//						task.start(clientId);
//						
//						Client.logMessage("Execute task '" + task.toString() + "'");
//						Logger.debug("Task_Init,"+clientId+","+task.getNamespace()+":"+task.getIndex());
//
//						// execute task
//						task.setBaseDir(workingDir);
//						taskInterface.prepareWorker(task);
//
//						if (taskInterface.runSetupScript(task)) {
//							taskInterface.runTask(task);
//						} else {
//							// wait some time
//							waitSomeTime(task.getWaitAfterSetupError(), waitAfterSetupError);
//
//							throw new TaskException("Setup script did not exit with 0.");
//						}
//						
//						Logger.debug("Task_Success,"+clientId+","+task.getNamespace()+":"+task.getIndex());
//						
//						// wait some time
//						waitSomeTime(task.getWaitAfterSuccess(), waitAfterSuccess);
//					} catch (TaskException e) {
//						taskInterface.fail(task, e.getMessage());
//
//						// wait some time
//						waitSomeTime(task.getWaitAfterRunError(), waitAfterRunError);
//					} catch (SetupException e) {
//						taskInterface.fail(task, e.getMessage());
//
//						// wait some time
//						waitSomeTime(task.getWaitAfterSetupError(), waitAfterSetupError);
//					}
//				}
//			} catch (InterruptedException e) {
//				Client.logError("Error while waiting for task: " + e.getMessage());
//				break;
//			}
//
//			// check if the client should be shut down
//			if (shutdown != null) {
//				if (start.plusMillis(shutdown).isBeforeNow()) {
//					Client.logMessage("Shutting down.");
//					Logger.debug("Client_shutdown,"+clientId);
//					break;
//				}
//			}
//		}
//	}

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
			Integer waitTime2) {
		try {
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
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
