package de.tuda.p2p.tdf.cmd;

import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import argo.jdom.JsonNodeType;
import de.tuda.p2p.tdf.common.Namespace;
import de.tuda.p2p.tdf.common.RedisHash;
import de.tuda.p2p.tdf.common.TaskSetting;


public class AddNamespace extends CMD{
	

	private static String addNamespace(JsonNode jn){
		if (!jn.hasFields()) return null;
		Namespace t = new Namespace();
		RedisHash rh = new RedisHash();
		for(JsonField i :  jn.getFieldList()) {
			if(i.getName().getText() != "Name"){
				t.setName(i.getValue().getText());
				break;
			}
			Object v = null;
			switch(i.getValue().getType()){
			case NUMBER: 
			case STRING: v= i.getValue().getText();
				break;
			case ARRAY:	 
				break;
			default: return null;
			}
			if (i.getValue().getType() !=JsonNodeType.ARRAY && i.getName().getText() != "Name")
				rh.put(TaskSetting.valueOf(i.getName().getText()),v.toString());
		}
		
		t.applyDefaults(rh);
		
		//debugging
		return t.asString();
		
		//t.save(jedis);
		//return t.getIndex().toString();
	}

	
	public static void main(String[] args){
		init();
		JsonNode jn = parsejson(getInput(args));
		if(jn.hasElements()){
			for (JsonNode j : jn.getElements()) System.out.println(addNamespace(j));
		}else{
			System.out.println(addNamespace(jn));
		}
	}

}
