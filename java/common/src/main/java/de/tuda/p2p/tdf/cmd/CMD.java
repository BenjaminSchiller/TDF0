package de.tuda.p2p.tdf.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;

import redis.clients.jedis.Jedis;
import argo.jdom.JdomParser;
import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;


public abstract class CMD {

	protected static Jedis jedis;
	
	public static void init(){
		
		//jedis=new Jedis(hostname);
	}
	
	protected static JsonNode parsejson(String s){
		JdomParser json=new JdomParser();
		JsonNode jn;
		try {
			jn=json.parse(s.replaceAll(System.lineSeparator(),"").replaceAll("\t",""));
		} catch (InvalidSyntaxException e) {
			System.err.println(e.getMessage());
			return null;	
		}
		return jn;
	}

	
	protected static String getInput(String[] args){
	String Input="";
	try {
	if(args.length == 1)
	try {
		Input = IOUtils.toString(new FileReader(new File(args[0])));
	} catch (FileNotFoundException e) {
		System.err.println("file " +args[0]+ " not found");
		System.exit(2);
	}
	else
		Input = IOUtils.toString(new InputStreamReader(System.in));
	}catch(IOException e){
		System.err.println(e.getMessage());
		System.exit(5);
	}
	return Input;
	}
}
