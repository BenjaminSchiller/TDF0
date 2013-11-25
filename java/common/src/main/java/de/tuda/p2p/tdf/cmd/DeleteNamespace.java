package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonNode;

public class DeleteNamespace extends CMD {
	
	public static void main(String[] args){
		delete(parsejson(getInput(args)));
	}

	
	public static boolean deleteNamespace(String identifier){
		String namespace=identifier.split("\\.")[1];
		
		for (String i : jedis.keys("tdf." + namespace + ".task.*")) 
			DeleteTask.deleteTask("tdf." + namespace + ".task." + i);
		for (String i : jedis.keys("tdf." + namespace + ".tasklist.*")) 
			DeleteTaskList.deleteTaskList("tdf." + namespace + ".tasklist." + i);
		
		
		// keys to be deleted from database, they are going to be prefixed with
		// '"tdf." + namespace + "."'
		String[] keys = 
			{ 	"queuing", 
				"running", 
				"completed", 
				"processed",
				"index"
				};
		
		for (String i : keys) jedis.del("tdf." + namespace + "."+i);
		
		jedis.srem("tdf.namespaces",namespace);
		return true;
	}
	
	
	private static void delete(JsonNode jn) {
		for (JsonNode cn : jn.getElements()){
			deleteNamespace(cn.getText());
		}
	}

}
