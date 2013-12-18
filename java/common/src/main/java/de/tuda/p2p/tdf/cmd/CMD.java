package de.tuda.p2p.tdf.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import redis.clients.jedis.Jedis;
import argo.jdom.JdomParser;
import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;


public abstract class CMD {

	protected static Jedis jedis;
	protected static Map<String,String> Settings =new HashMap<>(); 
	
	public static void init() {
		jedis = new Jedis("localhost");
		JsonNode jn = null;
		try {
			jn = parsejson(IOUtils.toString(new FileReader("settings")));
		} catch (IOException e) {
			err("Config file: " + e.getMessage());
			System.exit(29);
			;
		}
		for (JsonField jf : jn.getFieldList())
			Settings.put(jf.getName().getText(), jf.getValue().getText());
		if (Settings.containsKey("hostname")) {
			if (Settings.containsKey("port"))
				jedis = new Jedis(Settings.get("hostname"),
						Integer.valueOf(Settings.get("port")));
			else
				jedis = new Jedis(Settings.get("hostname"));
		}
		// if (Settings.containsKey("user"))
		// if (Settings.containsKey("password"))
		// if (Settings.containsKey("loglevel"))

	}
	
	private static void err(String string) {
		System.err.println(string);
	}

	protected static JsonNode parsejson(String s){
		JdomParser json=new JdomParser();
		JsonNode jn;
		try {
			jn=json.parse(s.replaceAll(System.lineSeparator(),"").replaceAll("\t",""));
		} catch (InvalidSyntaxException e) {
			err(e.getMessage());
			return null;	
		}
		return jn;
	}

	
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
