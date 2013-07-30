package de.tuda.p2p.tdf.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import javax.naming.ConfigurationException;

import org.joda.time.DateTime;

import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.server.TaskInterfaceServer;
import de.tuda.p2p.tdf.server.TaskInterfaceServerFactory;

public class Stock {

	private static TaskInterfaceServer taskInterface;

	public static void main(String... args) {
			try {
				taskInterface = TaskInterfaceServerFactory.create();

				String dax = "http://www.boerse.de/indizes/DAX/DE0008469008";
				String mdax = "http://www.boerse.de/indizes/MDAX/DE0008467416";
				String tecdax = "http://www.boerse.de/indizes/TecDAX/DE0007203275";
				String dowjones = "http://www.boerse.de/indizes/Dow-Jones-Industrial-Average/US2605661048";
				String nasdaq = "http://www.boerse.de/indizes/Nasdaq-100/US6311011026";

				// create tasks for the next 10 minutes
				for (int i=0; i<10; i++) {
					createTask(dax, DateTime.now().plusMinutes(i));
					createTask(mdax, DateTime.now().plusMinutes(i));
					createTask(tecdax, DateTime.now().plusMinutes(i));
					createTask(dowjones, DateTime.now().plusMinutes(i));
					createTask(nasdaq, DateTime.now().plusMinutes(i));
				}
			} catch (FileNotFoundException e) {
				System.err.println("Could not find config file 'db.properties'.");
			} catch (URISyntaxException e) {
				System.err.println("Error while loading config file.");
			} catch (IOException e) {
				System.err.println("Error while reading config file.");
			} catch (ConfigurationException e) {
				System.err.println(e.getMessage());
			}
	}

	private static void createTask(String input, DateTime runAfter) {
		Task task = new Task();
		task.setInput(input);
		task.setRunAfter(runAfter);
		task.setWorker("http://dl.dropbox.com/u/1721583/stock-worker-1.0.0.zip");
		task.setTimeout(10000);
		taskInterface.addTask("stock", task);
	}
}
