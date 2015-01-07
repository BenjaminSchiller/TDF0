package de.tuda.p2p.tdf.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class RetrieveClientLog extends CMD {
	public static void main(String[] args){
		init();
		
		Options options = new Options();
		options.addOption("c", true, "Client to retrieve logs from");
		options.addOption("b", false, "Receive logs blockingly");
		options.addOption("l", false, "List clients that have logs available");
		options.addOption("h", false, "show help");
	
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
		
		if(cmd.hasOption("h") || (cmd.hasOption("b") && cmd.hasOption("l")) ||
				(cmd.hasOption("c") && cmd.hasOption("l")) ||
				(!cmd.hasOption("c") && !cmd.hasOption("l") && !cmd.hasOption("h"))) {
			System.err.println("Missing parameter!");
			formatter.printHelp("{-c <client> [-b]} | -l | -h", options);
			System.exit(-1);
	
		}
		
		if(cmd.hasOption("l")) {
			for(String client : dbFactory.listClientsWithLogs()) {
				System.out.println(client);
			}
			System.exit(0);
		}
		else  {
			if(cmd.hasOption("b")) {
				while(true) {
					System.out.println(dbFactory.getNextLogMessageBlocking(cmd.getOptionValue("c"))); }
			}
			else {
				String logMsg = dbFactory.getNextLogMessage(cmd.getOptionValue("c"));
				while(logMsg != null) {
					System.out.println(logMsg);
					logMsg = dbFactory.getNextLogMessage(cmd.getOptionValue("c"));
				}
			}
		}
	}
}
