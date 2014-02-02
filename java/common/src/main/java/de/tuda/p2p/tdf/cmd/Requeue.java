package de.tuda.p2p.tdf.cmd;

import java.io.FileNotFoundException;
import java.util.LinkedList;

import de.tuda.p2p.tdf.common.Task;
import de.tuda.p2p.tdf.common.TaskList;

public class Requeue extends CMD {
	
	private static LinkedList<TaskList> Tasklists = new LinkedList<TaskList>();
	
	public static void main(String[] args){
		init();
		requeue();
		int count=0;
		for (TaskList t : Tasklists) {
			count+= t.getTasks().size();
		}
		say("requeued " + count + " tasks in "+ Tasklists.size() + " lists");
	}
	public static void requeue(){
		for (String namespace : jedis.smembers("tdf.namespaces")) requeue(namespace);
		for (TaskList t : Tasklists) {
			t.requeue();
		}
	}
	public static void requeue(String namespace) {
		
		// check the new set
		for (String index : jedis.smembers("tdf." + namespace + ".new")) {

			Task task;
			try {
				task = new Task(jedis,namespace, Long.valueOf(index));
			} catch ( FileNotFoundException e) {
				break; // TODO Log
			} 

			if (task != null) {
				requeue(task);
				

				
			}
		}
		// check the running set
		for (String index : jedis.smembers("tdf." + namespace + ".running")) {

			Task task;
			try {
				task = new Task(jedis,namespace, Long.valueOf(index));
			} catch ( FileNotFoundException e) {
				break; // TODO Log
			} 

			if (task != null && task.isTimedOut()) {
				requeue(task);
				;

				
			}
		}
		
		return ;
	}
	private static void requeue(Task task) {
		if (filter(task.getNamespace(),Tasklists).size() == 0 
				|| filter(task.getNamespace(),Tasklists).getFirst().getTasks().size() > 100) 
			Tasklists.push(new TaskList(jedis, task.getNamespace()));
		filter(task.getNamespace(),Tasklists).getFirst().addtask(task);
		
	}
	private static LinkedList<TaskList> filter(String namespace,
			LinkedList<TaskList> tasklists) {
		LinkedList<TaskList> r = new LinkedList<TaskList>();
		for (TaskList t : tasklists) {
			if (t.getNamespace() == namespace) r.addLast(t);
		}
		return r;
	}

}
