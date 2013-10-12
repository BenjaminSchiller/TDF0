/**
 * 
 */
package de.tuda.p2p.tdf.common;

import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;

/**
 * @author georg
 *
 */ 
public class TaskList extends Task {
	
	private Set<Task> tasks = new HashSet<Task>();

	/**
	 * @param jedis
	 * @param namespace
	 * @param index
	 */
	public TaskList(Jedis jedis, String namespace, Long index) {
		super(jedis, namespace, index);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	public TaskList() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param worker
	 * @param input
	 * @param runBefore
	 * @param runAfter
	 * @param timeout
	 * @param waitAfterSetupError
	 * @param waitAfterRunError
	 * @param waitAfterSuccess
	 * @param session
	 */
	public TaskList(String worker, String input, DateTime runBefore,
			DateTime runAfter, String timeout, String waitAfterSetupError,
			String waitAfterRunError, String waitAfterSuccess, String session) {
		super(worker, input, runBefore, runAfter, timeout, waitAfterSetupError,
				waitAfterRunError, waitAfterSuccess, session);
		// TODO Auto-generated constructor stub
	}
	
	private String ListKey(String namespace, Long index) {
		return "tdf." + namespace + ".tasklist." + index;
	}

	@SuppressWarnings("unused")
	private String HashKey(String namespace, Long index) {
		return ListKey(namespace,index)+".defaults";
	}
	
	private String SetKey(String namespace, Long index) {
		return ListKey(namespace, index)+".tasks";
	}

	/**
	 * 
	 * @param task the Task to be added
	 * @return the number of Tasks added (0 or 1)
	 */
	public long addtask(Task task){
		if (tasks.add(task)) return 1L;
		
		return 0L;
	}
	/**
	 * 
	 * @param set a Set of tasks to be added
	 * @return the number of Tasks added <=set.size()
	 */
	public long addtask(Set<Task> set){
		int counter=0;
		
		for (Task t : set) {
			counter+=addtask(t);
		}
		
		return counter;
	}
	
	@Override
	public Long save(Jedis jedis, String namespace, Long index){
		
		for (Task t : tasks){
			t.save(jedis, namespace);
			jedis.sadd(SetKey(namespace,index), String.valueOf(t.getIndex()));
		}
		
		//save defaults
		return super.save(jedis,namespace,index);
		
	}
}
