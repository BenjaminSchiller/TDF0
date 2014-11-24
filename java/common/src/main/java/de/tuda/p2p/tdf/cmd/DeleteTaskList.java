package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonNode;

/**
 * To be discussed if still needed/how to implement
 * maybe just give key and it will be deleted (AddTaskList has to give back key...)
 * @author jan
 *
 */
public class DeleteTaskList extends CMD {
	
	public static void main(String[] args){
		init();
		if(args.length != 1) {
			System.err.println("Please provide a database key of a task list to delete!");
			System.exit(1);
		}
		
		if(dbFactory.deleteTaskList(args[0]))
			System.exit(0);
		else {
			System.err.println("Database key " + args[0] + " not found!");
			System.exit(-1);
		}
	}

}
