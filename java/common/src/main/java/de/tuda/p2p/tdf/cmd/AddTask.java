package de.tuda.p2p.tdf.cmd;

import redis.clients.jedis.Jedis;
import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import de.tuda.p2p.tdf.common.RedisHash;
import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.common.TaskSetting;


public class AddTask extends CMD{
	
	@SuppressWarnings("unused")
	private static Jedis jedis;

	private static String add(JsonNode jn){
		if (!jn.hasFields()) return null;
		Task t = new Task();
		RedisHash rh = new RedisHash();
		for(JsonField i :  jn.getFieldList()) {
			if(i.getName().getText() == "ID"){
				t.setIndex(Long.valueOf(i.getValue().getText()));
				break;
			}
			Object v = null;
			switch(i.getValue().getType()){
			case NUMBER: 
			case STRING: v= i.getValue().getText();
				break;
			default: return null;
			}
			rh.put(TaskSetting.valueOf(i.getName().getText()),v.toString());
		}
		t.applyDefaults(rh);

		//debugging
		return t.asJsonString();
		
		//t.save(jedis);
		//return t.getIndex().toString();
	}
	
	public static void main(String[] args){
		init();
		say(Settings.toString());
		JsonNode jn = parsejson(getInput(args));
		if(jn.hasElements()){
			for (JsonNode j : jn.getElements()) System.out.println(add(j));
		}else{
			System.out.println(add(jn));
		}
	}

}
