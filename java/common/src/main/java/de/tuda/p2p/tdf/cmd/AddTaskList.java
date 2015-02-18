package de.tuda.p2p.tdf.cmd;

import java.util.Vector;

import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;
import de.tuda.p2p.tdf.common.NamespaceNotExistant;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;
import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;


public class AddTaskList extends CMD{
	
	public static String namespace = "";

	
	public static void main(String[] args){
		init();
		
		try {
			JsonNode jsonTree = DatabaseFactory.parseJson(getInput(args));
			
			Vector<Task> tl = new Vector<Task>();
			
			
			if(jsonTree.getElements().get(0).isArrayNode()) {
				for(JsonNode sublist : jsonTree.getElements()) {
					TaskList taskl = dbFactory.generateTaskList(createTaskList(sublist), namespace);
					say(taskl.getDBKey());
				}
				System.exit(0);
			}
			
			else {
				TaskList taskl = dbFactory.generateTaskList(createTaskList(jsonTree), namespace);
				say(taskl.getDBKey());
			}
			
			System.exit(0);

		} catch (InvalidSyntaxException e) {
			System.err.println("Error Reading Json-Input!");
			e.printStackTrace();
		} catch (NamespaceNotExistant e) {
			e.printStackTrace();
			System.err.println("Namespace does not exist!");
			System.exit(1);
		}
	}
	
	public static Vector<Task> createTaskList(JsonNode jsonTree) throws InvalidSyntaxException {
		Vector<Task> tl = new Vector<Task>();

		for(JsonNode jn : jsonTree.getElements()) {
			Task t = new Task();
			t.loadFromJson(jn);
			
			tl.add(t);
			namespace = t.getNamespace();
		}
		return tl;
	}

}
