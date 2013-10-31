package de.tuda.p2p.tdf.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;


public abstract class CMD {

	
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
