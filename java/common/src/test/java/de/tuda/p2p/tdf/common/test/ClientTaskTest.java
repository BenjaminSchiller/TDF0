package de.tuda.p2p.tdf.common.test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import de.tuda.p2p.tdf.common.ClientTask;

public class ClientTaskTest {

	DateTimeFormatter formatter = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss");

	@Test
	public void testSetter() {
		ClientTask task = new ClientTask();

		DateTime dt = DateTime.now();

		task.setOutput("output");
		task.setLog("log");
		task.setError("error");
		task.setClient("client");
		task.setStarted(dt);
		task.setFinished(dt.plusMinutes(1));

		assertThat(task.getOutput(), is("output"));
		assertThat(task.getLog(), is("log"));
		assertThat(task.getError(), is("error"));
		assertThat(task.getClient(), is("client"));
		assertThat(task.getStartedAsString(), is(dt.toString(formatter)));
		assertThat(task.getFinishedAsString(),
				is(dt.plusMinutes(1).toString(formatter)));
	}

}
