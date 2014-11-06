package de.tuda.p2p.tdf.cmd;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import de.tuda.p2p.tdf.common.redisEngine.DatabaseStringQueue;

public class QueueSingleTasks extends CMD {
	
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
		
		Collection<String> tasks = dbFactory.getSingleTasks(namespace).getAllCurrent();
		Integer tasksize = tasks.size();
		
		if(!equally) {
			Iterator<String> iter = tasks.iterator();
			while(tasksize > listsize) {
				List<String> taskl = new LinkedList<String>();
				for(int i = 0; i < listsize; i++) {
					String t = iter.next();
					taskl.add(t);
					tasksize--;
				}
				dbFactory.generateTaskListExisting(taskl, namespace);
			}
			
			List<String> taskl = new LinkedList<String>();
			while(tasksize !=0)
			{
				String t = iter.next();
				taskl.add(t);
				tasksize--;
			}
			dbFactory.generateTaskListExisting(taskl, namespace);
		}
		else {
			
		}
	}
}
