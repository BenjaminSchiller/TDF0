package de.tuda.p2p.tdf.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.Properties;
import java.io.FileInputStream;


import javax.naming.ConfigurationException;

import org.apache.commons.io.IOUtils;

import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;

import redis.clients.jedis.Jedis;
import argo.jdom.JdomParser;
import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;


public abstract class CMD {

	protected static DatabaseFactory dbFactory;
	protected static Map<String,String> Settings =new HashMap<String,String>(); 
	
	public static void init() {

		try {
             dbFactory = loadConfig();
        }
        catch (FileNotFoundException e) {
			System.out.println("Could not find config file 'cmd.properties'.");
			System.exit(29);
		} catch (URISyntaxException e) {
			System.out.println("Error while loading config file.");
			System.exit(29);
		} catch (IOException e) {
			System.out.println("Error while reading config file.");
			System.exit(29);
		} catch (ConfigurationException e) {
			System.out.println(e.getMessage());
			System.exit(29);
		} /*catch (JedisConnectionException e) {
			System.out.println("Error while connecting to Redis: " + e.getMessage());
		} catch (JedisDataException e) {
			System.out.println("Redis error: " + e.getMessage());
		}*/

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
	private static DatabaseFactory loadConfig() throws URISyntaxException,
			FileNotFoundException, IOException, ConfigurationException {

		File configFile = new File("./", "cmd.properties");

		Properties config = new Properties();
		System.out.println("Use config file: " + configFile.getCanonicalPath());
		config.load(new FileInputStream(configFile));
		
		String hostname = config.getProperty("redis.host");
		String port = config.getProperty("redis.port");
		String index = config.getProperty("redis.index");
		String password = config.getProperty("redis.auth");
		
		DatabaseFactory dbF = new DatabaseFactory(hostname, port, index, password);

		return dbF;
	}
	
	private static void err(String string) {
		System.err.println(string);
	}

/*	protected static JsonNode parsejson(String s){
		JdomParser json=new JdomParser();
		JsonNode jn;
		try {
			jn=json.parse(s.replaceAll(System.lineSeparator(),"").replaceAll("\t",""));
		} catch (InvalidSyntaxException e) {
			err(e.getMessage());
			return null;	
		}
		return jn;
	}*/

	
	protected static String getInput(String[] args) {
		String Input = "";
		try {
			if (args.length == 1)
				try {
					Input = IOUtils.toString(new FileReader(new File(args[0])));
				} catch (FileNotFoundException e) {
					err("file " + args[0] + " not found");
					System.exit(2);
				}
			else
				Input = IOUtils.toString(new InputStreamReader(System.in));
		} catch (IOException e) {
			err(e.getMessage());
			System.exit(29);
		}
		return Input;
	}

	protected static void say(String asString) {
		System.out.println(asString);
	}
}
