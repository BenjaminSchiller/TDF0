package de.tuda.p2p.tdf.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.Properties;

import javax.naming.ConfigurationException;

public class TaskInterfaceServerFactory {

	/**
	 * Loads the configuration from a 'db.properties' file and returns a task
	 * interface server object
	 * 
	 * @return The task interface server object
	 * @throws URISyntaxException
	 *             Thrown if the path to the configuration file is not correct
	 * @throws ConfigurationException
	 *             Thrown if information in the configuration file is missing or
	 *             cannot be parsed
	 * @throws IOException
	 *             Thrown if there was an error while loading the configuration
	 *             file
	 * @throws FileNotFoundException
	 *             Thrown if the configuration file cannot be found
	 */
	public static TaskInterfaceServer create() throws URISyntaxException,
			FileNotFoundException, ConfigurationException, IOException {
		CodeSource codeSource = TaskInterfaceServerFactory.class.getProtectionDomain().getCodeSource();
		File jarFile = new File(codeSource.getLocation().toURI().getPath());
		File jarDir = jarFile.getParentFile();
		File configFile;

		if (jarDir != null && jarDir.isDirectory()) {
			configFile = new File(jarDir, "db.properties");
		} else {
			throw new FileNotFoundException();
		}

		Properties config = new Properties();
		config.load(new FileInputStream(configFile));

		String host = config.getProperty("redis.host");
		if (host == null || host.isEmpty()) {
			throw new ConfigurationException(
					"Configuration file does not contatin 'redis.host' information.");
		}

		Integer port;
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

		Integer index;
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

		String auth = config.getProperty("redis.auth");
		if (auth == null) {
			throw new ConfigurationException(
					"Configuration file does not contatin 'redis.auth' information.");
		}

		if (auth.isEmpty()) {
			return new TaskInterfaceServer(host, port, index);
		}
		return new TaskInterfaceServer(host, port, index, auth);
	}

}
