package de.tuda.p2p.tdf.cmd;

import java.io.FileNotFoundException;

import argo.jdom.JsonNode;
import de.tuda.p2p.tdf.common.databaseObjects.Namespace;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;

/**
 * Unused at the moment!
 * @author jan
 *
 */
public class Show extends CMD {
	
	public static void main(String[] args){
		init();
		//show(parsesjson(getInput(args)));
	}
/*	
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
		String type,index;
		if(identifier.split("\\:").length==2){
			type="namespace";
			index="0";
		}else{
			index=identifier.split("\\:")[3];
			type = identifier.split("\\:")[2];
		}
		
		switch(type){
		case "task":
			try{
			say(new Task(jedis,namespace,Long.parseLong(index)).asString());
			}catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				say(e.getMessage());
			}
			break;
		case "tasklist":
			try {
				say(new TaskList(jedis,namespace,Long.parseLong(index)).asString());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				say(e.getMessage());
			}
			break;
		case "namespace":
			say(new Namespace(jedis,namespace).asString());
			break;
		}
		
	}
*/
}
