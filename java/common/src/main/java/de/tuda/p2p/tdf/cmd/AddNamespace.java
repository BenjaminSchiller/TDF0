package de.tuda.p2p.tdf.cmd;

import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;
import argo.jdom.JsonNode;
import argo.jdom.JsonField;
import argo.saj.InvalidSyntaxException;


public class AddNamespace extends CMD{
	
	public static void main(String[] args){
		init();
		
		String namespace = "";
		
		try {
			JsonNode jn = DatabaseFactory.parseJson(getInput(args));
			for(JsonField field : jn.getFieldList()) {
				if(field.getName().getText().equals("name"))
					namespace = field.getValue().getText();
			}
		} catch (InvalidSyntaxException e) {
			System.err.println("Error Reading Json-Input!");
			e.printStackTrace();
			System.exit(1);
		}
		
		if(dbFactory.namespaceExists(namespace)) {
			System.err.println("Namespace " + namespace + " does already exist!");
			System.exit(1);
		}
		else {
			dbFactory.addNamespace(namespace);
		}
	}

}
	