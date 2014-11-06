package de.tuda.p2p.tdf.cmd;

import java.util.Vector;

import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;


public class AddTaskList extends CMD{
	
	public static void main(String[] args){
		init();
		
		try {
			JsonNode jsonTree = DatabaseFactory.parseJson(getInput(args));
			
			Vector<Task> tl = new Vector<Task>();
			
			String namespace = "";
			
			for(JsonNode jn : jsonTree.getElements()) {
				Task t = new Task();
				try {
					t.loadFromJson(jn);
				} catch (InvalidSyntaxException e) {
					System.err.println("Error Reading Json-Input!");
					e.printStackTrace();
				}
				tl.add(t);
				namespace = t.getNamespace();
			}
			
			dbFactory.generateTaskList(tl, namespace);
		} catch (InvalidSyntaxException e) {
			System.err.println("Error Reading Json-Input!");
			e.printStackTrace();
		}
	}

}
