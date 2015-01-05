package de.tuda.p2p.tdf.common.databaseObjects;

import de.tuda.p2p.tdf.common.redisEngine.DatabaseHashObject;
import org.joda.time.DateTime;


public class TaskListMetainformation extends DatabaseHashObject {

	@Override
	protected void generateFields() {
		this.addFieldDeclaration("started", DateTime.class);
		this.addFieldDeclaration("maxRuntime", Integer.class);
	}

}
