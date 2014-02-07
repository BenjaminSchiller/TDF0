package de.tuda.p2p.tdf.cmd;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.sun.xml.internal.ws.api.pipe.NextAction;

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
			t.save(jedis);
			t.requeue();
		}
	}
	public static void requeue(String namespace) {
		
		class TaskComparator implements Comparator<Task>{

			@Override
			public int compare(Task o1, Task o2) {
				return o1.getRunBefore().compareTo(o2.getRunBefore());
			}


		}
				
		boolean evenout = Settings.containsKey("evenout")?Settings.get("evenout").toUpperCase()=="TRUE":true;
		int listsize= Settings.containsKey("listsize")?Integer.parseInt(Settings.get("listsize")):100;
		
		say("evenout:"+evenout);
		say("listsize:"+listsize);
		
		LinkedList<Task> tasks = new LinkedList<>();
		int fails=0;
		for (String index : jedis.smembers("tdf." + namespace + ".new")){
				say("tdf." + namespace + ".task." + index);
				try {
					Task task=new Task(jedis, namespace, Long.getLong(index));
					tasks.addFirst(task);
				} catch (FileNotFoundException e) {
					fails++;
					
				}
			
		}

		for (String index : jedis.smembers("tdf." + namespace + ".running")){
			
				try {
					Task task = new Task(jedis, namespace, Long.getLong(index));
					if (task == null || !task.isTimedOut()) break;
					tasks.addFirst(task);
				} catch (FileNotFoundException e) {
					fails++;
}
			
		}
		say("fails:"+fails);
		say("tasks:"+tasks.size());
		java.util.Collections.sort(tasks, new TaskComparator());
		say("tasks:"+tasks.size());
		int size = evenout?(int) Math.ceil(tasks.size()/(Math.ceil(tasks.size()/listsize))):listsize;
		for (Task task : tasks	) requeue(task,size);
		
		return ;
	}

	private static void requeue(Task task, int desiredListSize) {
		if (filter(task.getNamespace(),Tasklists).size() == 0 
				|| filter(task.getNamespace(),Tasklists).getFirst().getTasks().size() > desiredListSize) 
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
