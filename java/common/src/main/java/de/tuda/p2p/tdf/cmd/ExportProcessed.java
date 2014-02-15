package de.tuda.p2p.tdf.cmd;

import de.tuda.p2p.tdf.common.Namespace;
import de.tuda.p2p.tdf.common.Task;



public class ExportProcessed extends CMD{
	
	
	public static void main(String[] args){
		init();
		say(exportProcessed());
	}

	public static String exportProcessed(){
		StringBuilder sb = new StringBuilder("[\n");
		for (String namespace : jedis.smembers("tdf.namespaces"))
			sb.append(exportProcessed(namespace)+",");
		if(sb.lastIndexOf(",")>0) sb.deleteCharAt(sb.lastIndexOf(","));
		sb.append("]\n");
		return sb.toString();
	}
	public static String exportProcessed(String Namespace){
		StringBuilder sb = new StringBuilder("[\n");
		for (Task t : (new Namespace(jedis, Namespace)).getProcessed())
			sb.append(t.toString()+",\n");
		if(sb.lastIndexOf(",")>0) sb.deleteCharAt(sb.lastIndexOf(","));
		sb.append("]\n");
		return sb.toString();
	}
}
