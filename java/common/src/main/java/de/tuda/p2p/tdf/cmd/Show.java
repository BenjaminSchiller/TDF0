package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonNode;
import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.common.TaskList;

public class Show extends CMD {
	
	public static void main(String[] args){
		show(parsejson(getInput(args)));
	}
	
	private static void show(JsonNode jn){
		if (jn.hasElements()){
			for ( JsonNode i :jn.getElements())
				show(i);
		}else{
			show(jn.getText().toString());
		}
	}

	private static void show(String identifier) {
		String namespace=identifier.split("\\.")[1];
		String index=identifier.split("\\.")[3];
		String type = identifier.split("\\.")[2];
		
		switch(type){
		case "task":
			say(new Task(jedis,namespace,Long.parseLong(index)).asString());
			break;
		case "tasklist":
			say(new TaskList(jedis,namespace,Long.parseLong(index)).asString());
			break;
		}
		
	}

	private static void say(String asString) {
		System.out.println(asString);
	}

}
