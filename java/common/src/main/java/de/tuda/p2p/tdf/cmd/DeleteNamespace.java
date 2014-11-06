package de.tuda.p2p.tdf.cmd;

import de.tuda.p2p.tdf.common.redisEngine.DatabaseFactory;
import argo.jdom.JsonNode;
import argo.jdom.JsonField;
import argo.saj.InvalidSyntaxException;

public class DeleteNamespace extends CMD {
	
	public static void main(String[] args){
		init();
		try {
			JsonNode jn = DatabaseFactory.parseJson(getInput(args));
			for(JsonField field : jn.getFieldList()) {
				if(field.getName().getText().equals("name"))
					dbFactory.deleteNamespace(field.getValue().getText());
			}
		} catch (InvalidSyntaxException e) {
			System.err.println("Error Reading Json-Input!");
			e.printStackTrace();
		}
	}
}
