package de.tuda.p2p.tdf.cmd;

import java.util.Collection;
import java.util.Set;

import de.tuda.p2p.tdf.common.databaseObjects.Namespace;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;


public class ExportProcessed extends CMD{
	
	
	public static void main(String[] args){
		init();
		
		Options options = new Options();
		options.addOption("n", true, "Namespace");
		options.addOption("u", false, "Only newly processed tasks");
		options.addOption("f", false, "List failed tasks");
		options.addOption("h", false, "Show help");


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
		
		if(cmd.hasOption("u") && cmd.hasOption("f")) {
			System.out.println("Only -u or -f as parameter!");
			ExportProcessed.printHelp(options);
			System.exit(1);
		}
		
		Collection<Task> tl = null;
		
		if(cmd.hasOption("u")) {
			if(namespace.isEmpty())
				tl = dbFactory.getNewlyProcessedTasks();
			else
				tl = dbFactory.getNewlyProcessedTasks(namespace);
		}
		else if(cmd.hasOption("f")) {
			if(namespace.isEmpty())
				tl = dbFactory.getFailedTasks();
			else
				tl = dbFactory.getFailedTasks(namespace);
		}
		else {
			if(namespace.isEmpty())
				tl = dbFactory.getProcessedTasks();
			else
				tl = dbFactory.getProcessedTasks(namespace);
		}
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("[");
		
		String separator = "";
		
		for(Task t : tl) {
			sb.append(separator);
			separator = ",\n";
			sb.append(t.toJson());
		}
		
		sb.append("]");
			
		say(sb.toString());
	}
	
	private static void printHelp(Options options) { 
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("[-n <namespace>] [-u | -f]", options);
	}
}
