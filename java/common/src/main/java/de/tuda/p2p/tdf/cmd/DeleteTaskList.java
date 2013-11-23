package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonNode;

public class DeleteTaskList extends CMD {
	
	public static void main(String[] args){
		delete(parsejson(getInput(args)));
	}

	public static boolean deleteTaskList(String identifier) {
		
		String namespace=identifier.split("\\.")[1];
		String index=identifier.split("\\.")[3];
		
		// delete task information
		Long result = jedis.del("tdf." + namespace + ".tasklist." + index);

		// delete task from queuing list and running, completed and processed set
		result += jedis.lrem("tdf." + namespace + ".queuing", 0, index.toString());
		result += jedis.srem("tdf." + namespace + ".running", index.toString());
		result += jedis.srem("tdf." + namespace + ".completed", index.toString());
		result += jedis.srem("tdf." + namespace + ".processed", index.toString());

		return (result == 2);
	}
	
	private static void delete(JsonNode jn) {
		for (JsonNode cn : jn.getElements()){
			deleteTaskList(cn.getText());
		}
	}

}
