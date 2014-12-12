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

import de.tuda.p2p.tdf.common.NamespaceNotExistant;
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
		options.addOption("k", true, "Size of the lists to generate");
		options.addOption("e", false, "Try to build equally great list, no sparse last list");

		String namespace = "";
		Long listsize = 0L;
		boolean equally = false;

		HelpFormatter formatter = new HelpFormatter();
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
				
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			say("Error parsing arguments");
			e.printStackTrace();
			System.exit(-1);
		}
		
		if(!cmd.hasOption("k") || !cmd.hasOption("n")) {
			System.err.println("Missing parameter!");
			formatter.printHelp("-n namespace -k number [-e]", options);
			System.exit(-1);

		}
		else {
			namespace = cmd.getOptionValue("n");
			listsize = Long.valueOf(cmd.getOptionValue("k"));
		}
		
		if(cmd.hasOption("e"))
			equally = true;
		
		
		Collection<TaskList> requeuedTaskLists;
		try {
			requeuedTaskLists = dbFactory.requeue(namespace, listsize, equally);
			
			for(TaskList tl : requeuedTaskLists)
				System.out.println(tl.getDBKey());
			
			System.exit(0);
			
		} catch (NamespaceNotExistant e) {
			e.printStackTrace();
			System.err.println("Namespace does not exist!");
			System.exit(1);
		}


	}
	
}
