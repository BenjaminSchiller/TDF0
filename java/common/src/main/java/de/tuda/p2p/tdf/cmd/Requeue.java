package de.tuda.p2p.tdf.cmd;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

//import com.sun.xml.internal.ws.api.pipe.NextAction;

import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;

/**
 * Check if still needed!
 * @author jan
 *
 */
public class Requeue extends CMD {
	
	private static LinkedList<TaskList> Tasklists = new LinkedList<TaskList>();
	
	public static void main(String[] args){
		init();
		
		Options options = new Options();
		options.addOption("n", true, "Namespace");

		String namespace = "";

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
				
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			say("Error parsing arguments");
			e.printStackTrace();
			System.exit(-1);
		}
		
		if(cmd.hasOption("n"))
			namespace = cmd.getOptionValue("n");
		
		Collection<String> requeued;
		
		if(namespace.isEmpty())
			requeued = dbFactory.requeue();
		else
			requeued = dbFactory.requeue(namespace);
		
		for(String key : requeued)
			System.out.println(key);
		
		System.exit(0);

	}
	
	private static void printHelp(Options options) { 
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("[-n namespace]", options);
	}
}
