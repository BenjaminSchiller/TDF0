package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import argo.jdom.JsonNodeType;
import argo.jdom.JsonObjectNodeBuilder;
import de.tuda.p2p.tdf.common.RedisHash;
import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.common.TaskList;
import de.tuda.p2p.tdf.common.TaskSetting;


public class AddTaskList extends CMD{
	

	private static String addTaskList(JsonNode jn){
		if (!jn.hasFields()) return null;
		TaskList t = new TaskList();
		RedisHash rh = new RedisHash();
		for (JsonField i : jn.getFieldList()) {
			if (i.getName().getText().matches("(?i)ID")) {
				t.setIndex(Long.valueOf(i.getValue().getText()));
				;
			} else if (i.getName().getText().matches("(?i)Namespace")) {
				t.setNamespace(i.getValue().getText());
			} else if (i.getName().getText().matches("(?i)Namespace")) {
				for (JsonNode task : i.getValue().getElements())
					t.addtask(addTask(task));
				break;
			} else {
				Object v = null;
				switch (i.getValue().getType()) {
				case NUMBER:
				case STRING:
					v = i.getValue().getText();
					break;
				default:
					return null;
				}
				try {
					rh.put(TaskSetting.valueOf(i.getName().getText()),
							v.toString());
				} catch (java.lang.IllegalArgumentException e) {
					say(i.getName().getText());
					System.exit(1);
				}
				;
			}
		}
		
		t.applyDefaults(rh);
		
		return Long.toString( t.save(jedis),16);
	}

	

	private static Task addTask(JsonNode jn){
		if (!jn.hasFields()) return null;
		RedisHash rh = new RedisHash();
		Task t = new Task();
		for(JsonField i :  jn.getFieldList()) {
			if (i.getName().getText().matches("(?i)ID")) {
				t.setIndex(Long.valueOf(i.getValue().getText()));
				;
			} else if (i.getName().getText().matches("(?i)Namespace")) {
				t.setNamespace(i.getValue().getText());
			} else {
			
			Object v = null;
			switch(i.getValue().getType()){
			case NUMBER: 
			case STRING: v= i.getValue().getText();
				break;
			default: return null;
			}
			rh.put(TaskSetting.valueOf(i.getName().getText()),v.toString());
			}
		}
		t.applyDefaults(rh);

		return t;
		
	}
	
	public static void main(String[] args){
		init();
		JsonNode jn = parsejson(getInput(args));
		System.out.println(jn.hasFields());
		if(jn.hasElements()){
			for (JsonNode j : jn.getElements()) System.out.println(addTaskList(j));
		}else{
			System.out.println(addTaskList(jn));
		}
	}

}
