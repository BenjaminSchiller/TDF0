package de.tuda.p2p.tdf.cmd;

import java.util.Collection;
import java.util.Set;

import de.tuda.p2p.tdf.common.databaseObjects.Namespace;
import de.tuda.p2p.tdf.common.databaseObjects.Task;



public class ExportProcessed extends CMD{
	
	
	public static void main(String[] args){
		init();
		
		Collection<Task> tl = dbFactory.getProcessedTasks();
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("[");
		
		String separator = "";
		
		for(Task t : tl) {
			sb.append(separator);
			separator = ",";
			sb.append(t.toJson());
		}
		
		sb.append("]");
			
		say(sb.toString());
	}
}
