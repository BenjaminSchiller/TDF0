package de.tuda.p2p.tdf.server.test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.server.TaskInterfaceServer;

public class TaskInterfaceServerTest {

	private static TaskInterfaceServer taskInterface;

	DateTimeFormatter formatter = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss");

	/**
	 * Returns the database configuration
	 *
	 * @return The property object with the database configuration information
	 * @throws URISyntaxException
	 *             Thrown if the path to db.properties is not correct
	 * @throws FileNotFoundException
	 *             Thrown if the configuration file cannot be found
	 * @throws IOException
	 *             Thrown if there was an error while loading the configuration
	 */
	private static Properties getDatabaseConfig() throws URISyntaxException,
			FileNotFoundException, IOException {
		URL configUrl = TaskInterfaceServerTest.class
				.getResource("/db.properties");
		File configFile = new File(configUrl.toURI());

		Properties config = new Properties();
		config.load(new FileInputStream(configFile));

		return config;
	}

	@BeforeClass
	public static void connectToDatabase() throws URISyntaxException,
			FileNotFoundException, IOException {
		// get the database configuration from db.properties
		Properties config = getDatabaseConfig();

		String host = config.getProperty("redis.host");
		Integer port = Integer.valueOf(config.getProperty("redis.port"));
		Integer index = Integer.valueOf(config.getProperty("redis.index"));

		// connect to the test database
		taskInterface = new TaskInterfaceServer(host, port, index);

		// flush the test database
		taskInterface.getJedis().flushAll();
	}

	@Test
	public void testNamespaces() {
		// the initial list of namespaces should be empty
		assertThat(taskInterface.getNamespaces().isEmpty(), is(true));

		// add two namespaces; there should be two namespaces in the list
		taskInterface.addNamespace("foo");
		taskInterface.addNamespace("bar");
		assertThat(taskInterface.getNamespaces().size(), is(2));

		// change the index counter
		assertThat(taskInterface.getJedis().get("tdf.foo.index"), is("-1"));
		taskInterface.getJedis().set("tdf.foo.index", "1");

		// try to add the namespace "foo" again; it should not be added
		taskInterface.addNamespace("foo");
		assertThat(taskInterface.getNamespaces().size(), is(2));

		// index counter should still be "1"
		assertThat(taskInterface.getJedis().get("tdf.foo.index"), is("1"));

		// add task to "bar"
		Long index = taskInterface.addTask("bar", new Task("asd", "sdf", null, null, null, null, null, null, null));
		assertThat(taskInterface.countQueuingTasks("bar").intValue(), is(1));

		// remove the namespace "bar"; now there should be only one namespace
		// left
		taskInterface.deleteNamespace("bar");
		assertThat(taskInterface.getNamespaces().size(), is(1));

		// the task added to "bar" should not exist anymore
		assertThat(taskInterface.getTask("bar", index), equalTo(null));
	}

	@Test
	public void testAddAndRemoveTask() {
		// create task object
		Task task = new Task();
		task.setRunBefore(DateTime.now().plusHours(1));

		// try to add without worker
		Long index = taskInterface.addTask("foo", task);
		assertThat(index, is(-1L));
		task.setWorker("worker-uri");

		// add task to queue
		index = taskInterface.addTask("foo", task);
		assertThat(taskInterface.getQueuingTasks("foo").get(0).getWorker(),
				is("worker-uri"));

		// move task to running
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		assertThat(idx, equalTo(index.toString()));
		taskInterface.getJedis().sadd("tdf.foo.running", idx);
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(1));

		// move task to completed
		taskInterface.getJedis().smove("tdf.foo.running", "tdf.foo.completed",
				idx);
		assertThat(taskInterface.countCompletedTasks("foo").intValue(), is(1));

		// process task
		taskInterface.processTask("foo", index);
		assertThat(taskInterface.countCompletedTasks("foo").intValue(), is(0));
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(1));

		// update task
		task.setWorker("new-worker-uri");
		task.save(taskInterface.getJedis(), "foo", index);
		assertThat(taskInterface.getTask("foo", index).getWorker(),
				is("new-worker-uri"));

		// remove task
		assertThat(taskInterface.deleteTask("foo", index), is(true));
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(0));

		assertThat(taskInterface.getTask("foo", index), equalTo(null));
	}

	@Test
	public void testDeleteQueuingTasks() {
		// create task
		Task task = new Task();
		task.setWorker("worker-uri");

		// add it to queuing list
		Long index = taskInterface.addTask("foo", task);
		assertThat(taskInterface.countQueuingTasks("foo").intValue(), is(1));

		// delete all queuing tasks, list should be empty and task deleted
		assertThat(taskInterface.deleteQueuingTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countQueuingTasks("foo").intValue(), is(0));
		assertThat(taskInterface.getTask("foo", index), equalTo(null));
	}

	@Test
	public void testDeleteRunningTasks() {
		// create task
		Task task = new Task();
		task.setWorker("worker-uri");

		// move it to the running set
		Long index = taskInterface.addTask("foo", task);
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		taskInterface.getJedis().sadd("tdf.foo.running", idx);
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(1));

		// delete all running tasks, list should be empty and task deleted
		assertThat(taskInterface.deleteRunningTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(0));
		assertThat(taskInterface.getTask("foo", index), equalTo(null));
	}

	@Test
	public void testDeleteCompletedTasks() {
		// create task
		Task task = new Task();
		task.setWorker("worker-uri");

		// move it to the completed set
		Long index = taskInterface.addTask("foo", task);
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		taskInterface.getJedis().sadd("tdf.foo.completed", idx);
		assertThat(taskInterface.countCompletedTasks("foo").intValue(), is(1));

		// delete all completed tasks, list should be empty and task deleted
		assertThat(taskInterface.deleteCompletedTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countCompletedTasks("foo").intValue(), is(0));
		assertThat(taskInterface.getTask("foo", index), equalTo(null));
	}

	@Test
	public void testDeleteProcessedTasks() {
		// create task
		Task task = new Task();
		task.setWorker("worker-uri");

		// move it to the processed set
		Long index = taskInterface.addTask("foo", task);
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		taskInterface.getJedis().sadd("tdf.foo.processed", idx);
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(1));

		// delete all processed tasks, list should be empty and task deleted
		assertThat(taskInterface.deleteProcessedTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(0));
		assertThat(taskInterface.getTask("foo", index), equalTo(null));
	}

	@Test
	public void testAllTasks() {
		// create task
		Task task = new Task();
		task.setWorker("worker-uri");

		// move it to the processed set
		Long index = taskInterface.addTask("foo", task);
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		taskInterface.getJedis().sadd("tdf.foo.processed", idx);
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(1));

		// delete all tasks, processed list should be empty and task deleted
		assertThat(taskInterface.deleteAllTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(0));
		assertThat(taskInterface.getTask("foo", index), equalTo(null));
	}

	@Test
	public void testDeleteMissingTasks() {
		// add index to queuing
		taskInterface.getJedis().lpush("tdf.foo.queuing", "42");
		assertThat(taskInterface.countQueuingTasks("foo").intValue(), is(1));

		// delete missing tasks
		assertThat(taskInterface.deleteMissingTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countQueuingTasks("foo").intValue(), is(0));
	}

	@Test
	public void testRescheduling() {
		// create task and "start" it
		Task task = new Task();
		task.setWorker("worker-uri");
		task.setTimeout("100000");

		// add task to queue
		Long index = taskInterface.addTask("foo", task);

		// move task to running
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		assertThat(idx, equalTo(index.toString()));
		taskInterface.getJedis().sadd("tdf.foo.running", idx);
		taskInterface.getJedis().hset("tdf.foo.task." + idx, "started",
				DateTime.now().toString(formatter));

		// requeue, task should still be in the running set
		assertThat(taskInterface.requeue("foo").intValue(), is(0));
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(1));

		// change requeue time
		taskInterface.getJedis().hset("tdf.foo.task." + idx, "timeout", "-1");

		// requeue, task should be in the queuing list
		assertThat(taskInterface.requeue("foo").intValue(), is(1));
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(0));
		assertThat(taskInterface.countQueuingTasks("foo").intValue(), is(1));

		// change expire timestamp
		taskInterface.getJedis().hset("tdf.foo.task." + index, "runBefore",
				DateTime.now().minusMinutes(10).toString(formatter));

		// requeue (that includes checking for expired tasks)
		// task should still be in the queuing list
		assertThat(taskInterface.deleteAllExpiredTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countQueuingTasks("foo").intValue(), is(0));
	}

	@Test
	public void testExpireRunningTasks() {
		// create task and "start" it
		Task task = new Task();
		task.setWorker("worker-uri");
		task.setRunBefore(DateTime.now().plusMinutes(10));

		// add task to queue
		Long index = taskInterface.addTask("foo", task);

		// move task to running
		String idx = taskInterface.getJedis().lpop("tdf.foo.queuing");
		assertThat(idx, equalTo(index.toString()));
		taskInterface.getJedis().sadd("tdf.foo.running", idx);
		taskInterface.getJedis().hset("tdf.foo.task." + idx, "started",
				DateTime.now().toString(formatter));

		// requeue (that includes checking for expired tasks)
		// task should still be in the running set
		assertThat(taskInterface.requeue("foo").intValue(), is(0));
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(1));

		// change expire timestamp
		taskInterface.getJedis().hset("tdf.foo.task." + idx, "runBefore",
				DateTime.now().minusMinutes(10).toString(formatter));

		// requeue, running set should be empty
		assertThat(taskInterface.deleteAllExpiredTasks("foo").intValue(), is(1));
		assertThat(taskInterface.countRunningTasks("foo").intValue(), is(0));
	}

	@Test
	public void testExportTask() throws IOException {
		File directory = createTempDirectory();

		ClientTask task = new ClientTask();
		task.setWorker("worker-uri");
		task.setInput("input");
		task.setNamespace("foo");
		task.setBaseDir(directory);
		Long index = taskInterface.addTask("foo", task);
		assertThat(index.intValue(), is(not(-1)));

		taskInterface.getJedis().sadd("tdf.foo.processed", index.toString());
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(1));

		assertThat(taskInterface.exportProcessedTasks("foo", directory, true, true, true, true, true).intValue(), is(1));
		assertThat(taskInterface.countProcessedTasks("foo").intValue(), is(0));

		assertThat(task.getTaskIndexDir().listFiles().length, is(5));
	}

	private File createTempDirectory() throws IOException {
		File temp = File
				.createTempFile("tmp", Long.toString(System.nanoTime()));
		;
		if (!temp.delete()) {
			throw new IOException("Could not delete temp file: "
					+ temp.getCanonicalPath());
		}

		if (!temp.mkdirs()) {
			throw new IOException("Could not create temp directory: "
					+ temp.getCanonicalPath());
		}

		System.out.println("Using temp dir: " + temp.getCanonicalPath());
		return temp;
	}

}
