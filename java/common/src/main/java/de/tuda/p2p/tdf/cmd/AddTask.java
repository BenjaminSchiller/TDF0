package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import de.tuda.p2p.tdf.common.Namespace;
import de.tuda.p2p.tdf.common.RedisHash;
import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.common.TaskSetting;


public class AddTask extends CMD{
	

	private static String add(JsonNode jn){
		if (!jn.hasFields()) return null;
		
		Task t = new Task();
		t.loadFromJson(jn);
		
		if (t.getNamespace() == null || t.getNamespace().isEmpty()) return t.getIndex().toString()+": no namespace given, not added";
		
		t.save(jedis);
		
		jedis.sadd("tdf:"+t.getNamespace()+":new",t.getIndex().toString());
		System.out.println("fooRequeueing");
		Requeue.requeue();
		//t.requeue(jedis);
		return t.getIndex().toString();
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
