package de.tuda.p2p.tdf.cmd;

import java.util.ArrayList;
import java.util.List;

import argo.jdom.JsonNode;
import argo.jdom.JsonNodeBuilders;
import argo.jdom.JsonStringNode;

public class DeleteNamespace extends CMD {
	
	public static void main(String[] args){
		init();
		delete(parsejson(getInput(args)));
	}

	
	public static boolean deleteNamespace(String namespace){
		
		// keys to be deleted from database, they are going to be prefixed with
		// '"tdf." + namespace + "."'
		List<String> keys = new ArrayList<String>();
//		keys.add("tdf." + namespace + "."+"queuing");
//		keys.add("tdf." + namespace + "."+"running");
//		keys.add("tdf." + namespace + "."+"completed");
//		keys.add("tdf." + namespace + "."+"processed");
//		keys.add("tdf." + namespace + "."+"index");
//		keys.add("tdf." + namespace + "."+"new");
//		keys.addAll(jedis.keys("tdf." + namespace + ".task.*"));
//		keys.addAll(jedis.keys("tdf." + namespace + ".tasklist.*"));
		keys.addAll(jedis.keys("tdf." + namespace + "*"));
		jedis.del(keys.toArray(new String[0]));
		
		jedis.srem("tdf.namespaces",namespace);
		return true;
	}
	
	
	private static void delete(JsonNode jn) {
		if (jn.hasElements()) {
			for (JsonNode cn : jn.getElements()) {
				delete(cn);
			}
		} else if (jn.hasFields()) {
			JsonStringNode jsn = JsonNodeBuilders.aStringBuilder("name")
					.build();
			delete(jn.getFields().get(jsn));
		} else if (jn.hasText())
			deleteNamespace(jn.getText());
	}

}
