package de.tuda.p2p.tdf.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.ConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public class Client {

	private static String clientId;
	private static String host;
	private static Integer port;
	private static Integer index;
	private static Integer waitQueueEmpty;
	private static Integer waitQueueExpired;
	private static Integer waitAfterSuccess;
	private static Integer waitAfterSetupError;
	private static Integer waitAfterRunError;
	private static String auth;
	private static File workingDir;
	private static List<String> namespaces;
	private static boolean verbose = false;

	private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

	/**
	 * Starts the TDF client
	 *
	 * Possible command line arguments:
	 * -v , --verbose
	 * -h , --help
	 * -s <arg>, --shutdown <arg>
	 */
	public static void main(String... args) {
		try {
			loadConfig();
			// parse the command line options
			CommandLineParser parser = new PosixParser();
			CommandLine cmd = null;

			try {
			    cmd = parser.parse(createOptions(), args);
			} catch (ParseException e) {
			    // something bad happened so output help message
			    printHelp("Error in parsing arguments:n" + e.getMessage());
			}

			// parse shutdown time
			Integer shutdown = null;
			if (cmd != null && cmd.hasOption("shutdown")) {
				try {
					shutdown = Integer.valueOf(cmd.getOptionValue("shutdown"));
				} catch (NumberFormatException e) {
					printHelp("Not a valid value for the shutdown time in milliseconds: " + cmd.getOptionValue("shutdown"));
				}
			}

			// parse verbose log output
			if (cmd != null && cmd.hasOption("verbose")) {
				logMessage("Printing detailed log information...");
				verbose = true;
			}

			// parse display usage
			if (cmd != null && cmd.hasOption("help")) {
				printHelp(null);
			}
			Thread t = new TaskExecutor(waitQueueEmpty, waitQueueExpired, waitAfterSuccess,
					waitAfterSetupError, waitAfterRunError, shutdown, clientId, host,
					port, index, auth, workingDir, namespaces);
			t.run();
		} catch (FileNotFoundException e) {
			logError("Could not find config file 'client.properties'.");
		} catch (URISyntaxException e) {
			logError("Error while loading config file.");
		} catch (IOException e) {
			logError("Error while reading config file.");
		} catch (ConfigurationException e) {
			logError(e.getMessage());
		} catch (JedisConnectionException e) {
			logError("Error while connecting to Redis: " + e.getMessage());
		} catch (JedisDataException e) {
			logError("Redis error: " + e.getMessage());
		}
	}

	/**
	 * Create command line options
	 *
	 * @return the options object
	 */
	private static Options createOptions() {
		Options options = new Options();

		options.addOption("h", "help", false, "print this usage information");
		options.addOption("s", "shutdown", true, "shut down the client after <arg> time in milliseconds");
		options.addOption("v", "verbose", false, "print detailed log information");

		return options;
	}

	/**
	 * Prints the usage information
	 *
	 * @param message
	 *            additional information printed before the usage, can be null
	 */
	private static void printHelp(String message) {
		if (message != null) {
			System.err.println(message);
		}
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -jar tdf-client.jar [-h] [-s <arg>] [-v]", createOptions());
		System.exit(-1);
	}

	/**
	 * Loads the configuration from a 'client.properties' file
	 *
	 * @throws URISyntaxException
	 *             Thrown if the path to the configuration file is not correct
	 * @throws FileNotFoundException
	 *             Thrown if the configuration file cannot be found
	 * @throws IOException
	 *             Thrown if there was an error while loading the configuration
	 *             file
	 * @throws ConfigurationException
	 *             Thrown if information in the configuration file is missing or
	 *             can not be parsed
	 */
	private static void loadConfig() throws URISyntaxException,
			FileNotFoundException, IOException, ConfigurationException {
		CodeSource codeSource = Client.class.getProtectionDomain().getCodeSource();
		File jarFile = new File(codeSource.getLocation().toURI().getPath());
		File jarDir = jarFile.getParentFile();
		File configFile;

		if (jarDir != null && jarDir.isDirectory()) {
			configFile = new File(jarDir, "client.properties");
		} else {
			throw new FileNotFoundException();
		}

		Properties config = new Properties();
		config.load(new FileInputStream(configFile));

		logMessage("Use config file: " + configFile.getCanonicalPath());

		clientId = config.getProperty("client.id");
		if (clientId == null || clientId.isEmpty()) {
			throw new ConfigurationException(
					"Configuration file does not contatin 'client.id' information.");
		}

		host = config.getProperty("redis.host");
		if (host == null || host.isEmpty()) {
			throw new ConfigurationException(
					"Configuration file does not contatin 'redis.host' information.");
		}

		String redisPort = config.getProperty("redis.port");
		if (redisPort != null) {
			try {
				port = Integer.valueOf(redisPort);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + redisPort
						+ "' is not a valid value for 'redis.port'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'redis.port' information.");
		}

		String redisIndex = config.getProperty("redis.index");
		if (redisIndex != null) {
			try {
				index = Integer.valueOf(redisIndex);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + redisIndex
						+ "' is not a valid value for 'redis.index'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'redis.index' information.");
		}

		String waitQueueEmptyString = config.getProperty("wait.queue.empty");
		if (waitQueueEmptyString != null) {
			try {
				waitQueueEmpty = Integer.valueOf(waitQueueEmptyString);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + waitQueueEmptyString
						+ "' is not a valid value for 'wait.queue.empty'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'wait.queue.empty' information.");
		}

		String waitQueueExpiredString = config.getProperty("wait.queue.expired");
		if (waitQueueExpiredString != null) {
			try {
				waitQueueExpired = Integer.valueOf(waitQueueExpiredString);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + waitQueueExpiredString
						+ "' is not a valid value for 'wait.queue.expired'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'wait.queue.expired' information.");
		}

		String waitAfterSuccessString = config.getProperty("wait.success");
		if (waitAfterSuccessString != null) {
			try {
				waitAfterSuccess = Integer.valueOf(waitAfterSuccessString);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + waitAfterSuccessString
						+ "' is not a valid value for 'wait.success'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'wait.success' information.");
		}

		String waitAfterSetupErrorString = config.getProperty("wait.error.setup");
		if (waitAfterSetupErrorString != null) {
			try {
				waitAfterSetupError = Integer.valueOf(waitAfterSetupErrorString);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + waitAfterSetupErrorString
						+ "' is not a valid value for 'wait.error.setup'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'wait.error.setup' information.");
		}

		String waitAfterRunErrorString = config.getProperty("wait.error.run");
		if (waitAfterRunErrorString != null) {
			try {
				waitAfterRunError = Integer.valueOf(waitAfterRunErrorString);
			} catch (NumberFormatException e) {
				throw new ConfigurationException("'" + waitAfterRunErrorString
						+ "' is not a valid value for 'wait.error.run'.");
			}
		} else {
			throw new ConfigurationException(
					"Configuration file does not contatin 'wait.error.run' information.");
		}

		auth = config.getProperty("redis.auth");
		if (auth == null) {
			throw new ConfigurationException(
					"Configuration file does not contatin 'redis.auth' information.");
		}

		String workerDir = config.getProperty("worker.dir");
		if (workerDir == null || workerDir.isEmpty()) {
			throw new ConfigurationException(
					"Configuration file does not contatin 'worker.dir' information.");
		}
		workingDir = new File(workerDir);

		String namespacesString = config.getProperty("namespaces");
		namespaces = new ArrayList<String>();
		if ( ! (namespacesString == null)) {
			if ( ! namespacesString.isEmpty()) {
				for (String namespace : namespacesString.split(";")) {
					namespaces.add(namespace.trim());
				}
			}
		}
	}

	/**
	 * Writes the message with the current timestamp to STDOUT
	 *
	 * @param message
	 *            the log message
	 * @param debug
	 *            true if the message should only be printed in verbose mode
	 */
	public static void logMessage(String message, boolean debug) {
		if (debug) {
			//if (verbose) {
				System.out.println(DateTime.now().toString(formatter) + " | " + message);
			//}
		} else {
			System.out.println(DateTime.now().toString(formatter) + " | " + message);
		}
	}

	public static void logMessage(String message) {
		logMessage(message, false);
	}

	/**
	 * Writes the message with the current timestamp to STDERR
	 *
	 * @param message
	 *            the error message
	 * @param debug
	 *            true if the message should only be printed in verbose mode
	 */
	public static void logError(String message, boolean debug) {
		//Logger.log(message);
		if (debug) {
			if (verbose) {
				System.err.println(DateTime.now().toString(formatter) + " | " + message);
			}
		} else {
			System.err.println(DateTime.now().toString(formatter) + " | " + message);
		}
	}

	public static void logError(String message) {
		logError(message, false);
	}
}
