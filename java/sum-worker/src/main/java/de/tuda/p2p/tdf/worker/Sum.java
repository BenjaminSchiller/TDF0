package de.tuda.p2p.tdf.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import javax.naming.ConfigurationException;

import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.server.TaskInterfaceServer;
import de.tuda.p2p.tdf.server.TaskInterfaceServerFactory;

public class Sum {

	private static TaskInterfaceServer taskInterface;

	private static final Integer WAIT_TIME = 1000;

	private static NumberFormatter numberFormatter = new NumberFormatter();

	public static void main(String... args) {
		try {
			taskInterface = TaskInterfaceServerFactory.create();

			if (args.length != 3) {
				System.err.println("Run with start number, end number and interval as command line arguments.");
				return;
			}

			Long start = Long.valueOf(args[0]);
			Long end = Long.valueOf(args[1]);
			Long interval = Long.valueOf(args[2]);

			if (start > end) {
				System.err.println("End value must be greater than start value.");
				return;
			}
			if (interval > (end - start)) {
				System.err.println("Interval must be smaller than (end - start).");
				return;
			}

			// the first set of tasks
			Set<Long> taskIndexesToWaitFor = createInitialTasks(start, end, interval);

			// the iterations until only one task is left
			taskIndexesToWaitFor = taskIterations(interval, taskIndexesToWaitFor);

			// last task, wait to finish and display result
			System.out.println("Waiting for the final calculation.");

			Long taskIndex = taskIndexesToWaitFor.toArray(new Long[taskIndexesToWaitFor.size()])[0];
			Task task = taskInterface.getTask("sum", taskIndex);

			while ( ! task.isFinished()) {
				Thread.sleep(WAIT_TIME);
				task = taskInterface.getTask("sum", taskIndex);
			}

			System.out.println("The sum of numbers from " + start + " to " + end + " is " + task.getOutput().trim());

		} catch (NumberFormatException e) {
			System.err.println("Could not parse command line arguments.");
		} catch (FileNotFoundException e) {
			System.err.println("Could not find config file 'db.properties'.");
		} catch (URISyntaxException e) {
			System.err.println("Error while loading config file.");
		} catch (IOException e) {
			System.err.println("Error while reading config file.");
		} catch (ConfigurationException e) {
			System.err.println(e.getMessage());
		} catch (InterruptedException e) {
			System.err.println("Interrupted while waiting for tasks to finish.");
		}
	}

	/**
	 * Runs the task iterations, until only one task is left to calculate
	 * 
	 * @param interval
	 *            How many numbers should be added in one task
	 * @param taskIndexesToWaitFor
	 *            The indexes of all tasks that are currently running
	 * @return A set containing the index for the final task
	 * @throws InterruptedException
	 *             Thrown if the thread is interrupted while waiting for tasks
	 *             to finish
	 */
	private static Set<Long> taskIterations(Long interval,
			Set<Long> taskIndexesToWaitFor) throws InterruptedException {
		while (taskIndexesToWaitFor.size() > 1) {
			System.out.println("Waiting for " + taskIndexesToWaitFor.size() + " calculations.");

			numberFormatter.clear();

			Long counter = 0L;
			Set<Long> newTaskIndexesToWaitFor = new HashSet<Long>();

			Long i = 0L;
			for (Long taskIndex : taskIndexesToWaitFor) {
				i++;
				Task task = taskInterface.getTask("sum", taskIndex);

				// wait for each task to finish
				while (!task.isFinished()) {
					Thread.sleep(WAIT_TIME);
					task = taskInterface.getTask("sum", taskIndex);
				}

				// add the task's result to a new task
				numberFormatter.add(task.getOutput());

				if (counter > interval || i >= taskIndexesToWaitFor.size()) {
					// save the new task if it contains enough input
					newTaskIndexesToWaitFor.add(taskInterface.addTask("sum",
							createTaskWithInput(numberFormatter.toString())));

					numberFormatter.clear();
					counter = 0L;
				} else {
					counter++;
				}
			}
			taskIndexesToWaitFor = newTaskIndexesToWaitFor;
		}
		return taskIndexesToWaitFor;
	}

	/**
	 * Create the initial set of tasks
	 * 
	 * @param start
	 *            The start value
	 * @param end
	 *            The end value
	 * @param interval
	 *            The interval
	 * @return A set of task indexes
	 */
	private static Set<Long> createInitialTasks(Long start, Long end, Long interval) {
		Set<Long> taskIndexes = new HashSet<Long>();

		Long number = start;
		while (number <= end) {
			numberFormatter.clear();

			Long counter = 0L;
			while (counter < interval && number <= end) {
				numberFormatter.add(number);
				number++;
				counter++;
			}

			taskIndexes.add(taskInterface.addTask("sum", createTaskWithInput(numberFormatter.toString())));
		}

		return taskIndexes;
	}

	/**
	 * Create a task with input information
	 * 
	 * @param input
	 *            The String with input information
	 * @return The task object
	 */
	private static Task createTaskWithInput(String input) {
		Task task = new Task();

		task.setWorker("http://dl.dropbox.com/u/1721583/sum-worker-1.0.0.zip");
		task.setTimeout(60000);
		task.setInput(input);

		return task;
	}

}
