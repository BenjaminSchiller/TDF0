package de.tuda.p2p.tdf.common.test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import de.tuda.p2p.tdf.common.Task;

public class TaskTest {

	DateTimeFormatter formatter = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss");

	@Test
	public void testExpireCalculation() {
		Task task = new Task();

		task.setRunBefore(DateTime.now().plusHours(1).toString(formatter));
		assertThat(task.isExpired(), is(false));

		task.setRunBefore(DateTime.now().minusHours(1).toString(formatter));
		assertThat(task.isExpired(), is(true));
	}

	@Test
	public void testValidCalculation() {
		Task task = new Task();

		task.setRunAfter(DateTime.now().plusHours(1).toString(formatter));
		assertThat(task.isValid(), is(false));
		assertThat(Double.valueOf(task.validWaitTime()), is(closeTo(3600000, 1000)));

		task.setRunAfter(DateTime.now().minusHours(1).toString(formatter));
		assertThat(task.isValid(), is(true));
		assertThat(task.validWaitTime(), is(0L));
	}

	@Test
	public void testRequeueCalculation() {
		Task task = new Task();
		task.start("foo");

		// requeue 1 minute after start
		task.setTimeout("60000");
		assertThat(task.isTimedOut(), is(false));

		// requeue 1 minute before start (requeue anyway)
		task.setTimeout("-60000");
		assertThat(task.isTimedOut(), is(true));

		// run in one hour
		task.setRunAfter(DateTime.now().plusHours(1));
		assertThat(task.isTimedOut(), is(false));

		// run now
		task.setRunAfter(DateTime.now());
		assertThat(task.isTimedOut(), is (true));
	}

	@Test
	public void testParsingAndFormatting() {
		Task task = new Task();
		String runBefore = DateTime.now().toString(formatter);

		task.setRunBefore(runBefore);
		assertThat(task.getRunBeforeAsString(), equalTo(runBefore));
	}

	@Test
	public void testSession() {
		Task task = new Task();
		assertThat(task.getSession(), equalTo(DateTime.now().toString("yyyy-MM-dd")));
		task.setSession("");
		assertThat(task.getSession(), equalTo(DateTime.now().toString("yyyy-MM-dd")));
		task.setSession("1-2-3");
		assertThat(task.getSession(), equalTo("1-2-3"));
	}
}
