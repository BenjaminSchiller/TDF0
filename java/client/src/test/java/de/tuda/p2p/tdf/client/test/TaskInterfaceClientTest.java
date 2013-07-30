package de.tuda.p2p.tdf.client.test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuda.p2p.tdf.client.SetupException;
import de.tuda.p2p.tdf.client.TaskException;
import de.tuda.p2p.tdf.client.TaskInterfaceClient;
import de.tuda.p2p.tdf.common.ClientTask;
import de.tuda.p2p.tdf.common.Task;

public class TaskInterfaceClientTest {

	private static TaskInterfaceClient taskInterface;

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
		URL configUrl = TaskInterfaceClientTest.class
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
		taskInterface = new TaskInterfaceClient("test-client", host, port,
				index, "", null);

		// flush the test database
		taskInterface.getJedis().flushAll();

		// create namespace "foo"
		taskInterface.getJedis().sadd("tdf.namespaces", "foo");
		taskInterface.getJedis().set("tdf.foo.index", "-1");
	}

	@Test
	public void testGetNewTask() throws InterruptedException {
		// try to get a task
		assertThat(taskInterface.getTaskToExecute(30000), equalTo(null));

		// create expired task
		Task task = new Task();
		task.setWorker("worker-1");
		task.setIndex(taskInterface.getJedis().incr("tdf.foo.index"));
		task.setInput("test data\n");
		task.setRunBefore(DateTime.now().minusHours(1).toString(formatter));
		task.save(taskInterface.getJedis(), "foo");
		taskInterface.getJedis().rpush("tdf.foo.queuing",
				task.getIndex().toString());

		// create valid task
		task = new Task();
		task.setWorker("worker-2");
		task.setIndex(taskInterface.getJedis().incr("tdf.foo.index"));
		task.setInput("test data\n");
		task.save(taskInterface.getJedis(), "foo");
		taskInterface.getJedis().rpush("tdf.foo.queuing",
				task.getIndex().toString());

		task = taskInterface.getTaskToExecute(30000);
		assertThat(task.getClient(), is("test-client"));
		assertThat(task.getWorker(), is("worker-2"));
		assertThat(taskInterface.getJedis().llen("tdf.foo.queuing"), is(1L));
		assertThat(taskInterface.getJedis().scard("tdf.foo.running"), is(1L));
	}

	@Test
	public void testHttpRunner() throws IOException, TaskException, SetupException {
		Set<String> runningTasks = taskInterface.getJedis().smembers(
				"tdf.foo.running");
		assertThat(runningTasks.size(), is(1));
		ClientTask task = new ClientTask(taskInterface.getJedis(), "foo",
				Long.valueOf((String) runningTasks.toArray()[0]));

		task.setWorker("http://dl.dropbox.com/u/1721583/test-worker-1.1.0.zip");
		task.save(taskInterface.getJedis());

		task.setBaseDir(createTempDirectory());
		taskInterface.prepareWorker(task);

		// check for extracted setup.sh
		File[] files = new File(task.getWorkerDir(), "bin").listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().contains("setup.sh");
			}
		});
		assertThat(files.length, is(1));

		// check that zip file was deleted
		files = new File(task.getWorkerDir(), "bin").listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().contains("test-worker-1.1.0.zip");
			}
		});
		assertThat(files.length, is(0));

		// check directories
		assertThat(new File(task.getWorkerDir(), "temp").mkdirs(), is(false));
		assertThat(new File(task.getWorkerDir(), "tasks").mkdirs(), is(false));

		// execute task
		assertThat(taskInterface.runSetupScript(task), is(true));
		task = taskInterface.runTask(task);

		assertThat(task.getOutput(), equalTo("test data\n"));
		assertThat(task.getLog(), equalTo("setup log\nrun log\n"));
		assertThat(task.getError(), equalTo("setup error\nrun error\n"));
	}

	private File createTempDirectory() throws IOException {
		File temp = File
				.createTempFile("tmp", Long.toString(System.nanoTime()));

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

	@Test
	public void testFailTask() {
		taskInterface.getJedis().smove("tdf.foo.completed", "tdf.foo.running", "0");
		ClientTask task = new ClientTask(taskInterface.getJedis(), "foo", 0L);

		taskInterface.fail(task, "test error");
		assertThat(taskInterface.getJedis().scard("tdf.foo.running"), is(0L));
		assertThat(taskInterface.getJedis().scard("tdf.foo.completed"), is(1L));

		task = new ClientTask(taskInterface.getJedis(), "foo", task.getIndex());
		assertThat(task.getError(), equalTo("test error"));
	}

}
