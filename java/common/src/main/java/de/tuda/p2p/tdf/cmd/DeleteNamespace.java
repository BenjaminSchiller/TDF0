package de.tuda.p2p.tdf.cmd;

import java.util.ArrayList;
import java.util.List;

import argo.jdom.JsonNode;

public class DeleteNamespace extends CMD {
	
	public static void main(String[] args){
		delete(parsejson(getInput(args)));
	}

	
	public static boolean deleteNamespace(String namespace){
		
		for (String i : jedis.keys("tdf." + namespace + ".task.*")) 
			DeleteTask.deleteTask("tdf." + namespace + ".task." + i);
		for (String i : jedis.keys("tdf." + namespace + ".tasklist.*")) 
			DeleteTaskList.deleteTaskList("tdf." + namespace + ".tasklist." + i);
		
		
		// keys to be deleted from database, they are going to be prefixed with
		// '"tdf." + namespace + "."'
		List<String> keys = new ArrayList<>();
		keys.add("tdf." + namespace + "."+"queuing");
		keys.add("tdf." + namespace + "."+"running");
		keys.add("tdf." + namespace + "."+"completed");
		keys.add("tdf." + namespace + "."+"processed");
		keys.add("tdf." + namespace + "."+"index");
		keys.add("tdf." + namespace + "."+"new");
		keys.addAll(jedis.keys("tdf." + namespace + ".task.*"));
		keys.addAll(jedis.keys("tdf." + namespace + ".tasklist.*"));
		jedis.del(keys.toArray(new String[0]));
		
		jedis.srem("tdf.namespaces",namespace);
		return true;
	}
	
	
	private static void delete(JsonNode jn) {
		for (JsonNode cn : jn.getElements()){
			deleteNamespace(cn.getText());
		}
	}

}
