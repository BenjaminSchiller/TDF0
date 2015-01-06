package de.tuda.p2p.tdf.cmd;

import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import de.tuda.p2p.tdf.common.NamespaceNotExistant;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;


public class Timeout extends CMD {
	
	public static void main(String[] args){
		init();
		
		Options options = new Options();
		options.addOption("n", true, "Namespace");
		options.addOption("f", false, "Fail tasks of all timed out tasklists to run them again");
		options.addOption("g", true, "Fail tasks of specific timed out tasklists to run them again");
		options.addOption("h", false, "show help");

		String namespace = "";
		boolean failAll = false;


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
		
		if(cmd.hasOption("h") || !cmd.hasOption("n") || (cmd.hasOption("f") && cmd.hasOption("g"))) {
			System.err.println("Missing parameter!");
			formatter.printHelp("-n <namespace> [-f|-g <tasklist>]", options);
			System.exit(-1);

		}
		else {
			namespace = cmd.getOptionValue("n");
			if(cmd.hasOption("f")) {
				failAll = true;
			}
			if(cmd.hasOption("g")) {
				dbFactory.failTasklist(cmd.getOptionValue("g"));
				System.exit(0);
			}
		}
		
		try {
			Collection<TaskList> timedoutTasklists = dbFactory.getTimedoutTaskLists(namespace);
			for(TaskList tl : timedoutTasklists) {
				System.out.println(tl.getDBKey());
				if(failAll) {
					dbFactory.failTasklist(tl); }
			}
		} catch (NamespaceNotExistant e) {
			e.printStackTrace();
			System.err.println("Namespace does not exist!");
			System.exit(1);
		}
				
	}
}
