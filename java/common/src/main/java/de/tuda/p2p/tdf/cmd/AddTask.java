package de.tuda.p2p.tdf.cmd;

import argo.saj.InvalidSyntaxException;
import de.tuda.p2p.tdf.common.databaseObjects.Task;


public class AddTask extends CMD{
	
	public static void main(String[] args){
		init();
		
		try {
			Task t = new Task();
			t.loadFromJson(getInput(args));
			say(dbFactory.addSingleTask(t));
			
		} catch (InvalidSyntaxException e) {
			System.err.println("Error Reading Json-Input!");
			e.printStackTrace();
		}
		
	}

}
