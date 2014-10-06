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
		for (String namespace : jedis.smembers("tdf:namespaces"))
			sb.append(exportProcessed(namespace)+",\n");
		if(sb.lastIndexOf(",")>0) sb.deleteCharAt(sb.lastIndexOf(","));
		sb.append("]");
		return sb.toString();
	}
	public static String exportProcessed(String Namespace){
		StringBuilder sb = new StringBuilder("{\n\"name\": \""+Namespace+"\",\n");
		for (Task t : (new Namespace(jedis, Namespace)).getProcessed())
			sb.append(t.getIndex()+"\": \""+t.toString()+"\",\n");
		if(sb.lastIndexOf(",")>0) sb.deleteCharAt(sb.lastIndexOf(","));
		sb.append("}");
		return sb.toString();
	}
}
