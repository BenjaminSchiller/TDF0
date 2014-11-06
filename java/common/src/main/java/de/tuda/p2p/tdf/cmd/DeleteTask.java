package de.tuda.p2p.tdf.cmd;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import argo.jdom.JsonNode;

/**
 * To be discussed if still needed/how to implement
 * maybe just give key and it will be deleted (AddTaskList has to give back key...)
 * @author jan
 *
 */
public class DeleteTask extends CMD {
	
	public static void main(String[] args){
		init();		
/*		if(args.length == 2 && args[0].equals("-a")) {
			System.out.println("Deleting all tasks in namespace " + args[1]);
			deleteTasksNamespace(args[1]);
		}
		else {
			delete(parsejson(getInput(args)));
		}*/
	}
	
/*	public static boolean deleteTasksNamespace(String namespace)
	{
		Set<String> tasksSet = new HashSet<String>();
		Set<String> taskListsSet = new HashSet<String>();

		List<String> tasks = jedis.lrange("tdf:" + namespace + ":queuing", 0, jedis.llen("tdf:" + namespace + ":queuing"));
		
		if(tasks != null)
			tasksSet.addAll(tasks);
		
		List<String> taskLists = jedis.lrange("tdf:" + namespace + ":queuing", 0, jedis.llen("tdf:" + namespace + ":queuinglists"));
		if(taskLists != null)
			taskListsSet.addAll(taskLists);
		
		for(String task : tasksSet){
			jedis.del("tdf:" + namespace + ":task:" + task);
		}
		
		for(String taskList : taskListsSet){
			jedis.del("tdf:" + namespace + ":tasklist:" + taskList + ":tasks");
		}
		
		jedis.del("tdf:" + namespace + ":queuing", "tdf:" + namespace + ":queuinglists", "tdf:" + namespace + ":running", "tdf:" + namespace + ":completed", "tdf:" + namespace + ":processed");
				
		return true;
	}

	public static boolean deleteTask(String identifier) {
		
		String namespace=identifier.split("\\:")[1];
		String index=identifier.split("\\:")[3];
		
		// delete task information
		Long result = jedis.del("tdf:" + namespace + ":task:" + index);

		// delete task from queuing list and running, completed and processed set
		result += jedis.lrem("tdf:" + namespace + ":queuing", 0, index.toString());
		result += jedis.srem("tdf:" + namespace + ":running", index.toString());
		result += jedis.srem("tdf:" + namespace + ":completed", index.toString());
		result += jedis.srem("tdf:" + namespace + ":processed", index.toString());

		return (result == 2);
	}
	
	private static void delete(JsonNode jn) {
		for (JsonNode cn : jn.getElements()){
			deleteTask(cn.getText());
		}
	}*/

}
