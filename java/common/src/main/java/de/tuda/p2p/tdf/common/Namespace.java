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
public class Namespace extends Task {

	private Set<TaskList> lists = new HashSet<TaskList>();

	/**
	 * @param jedis
	 * @param namespace
	 * @param index
	 */
	public Namespace(Jedis jedis, String namespace, Long index) {
		super(jedis, namespace, index);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	public Namespace() {
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
	public Namespace(String worker, String input, DateTime runBefore,
			DateTime runAfter, String timeout, String waitAfterSetupError,
			String waitAfterRunError, String waitAfterSuccess, String session) {
		super(worker, input, runBefore, runAfter, timeout, waitAfterSetupError,
				waitAfterRunError, waitAfterSuccess, session);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Long save(Jedis jedis, String namespace, Long index){
		
		for (TaskList t : lists){
			t.save(jedis, namespace);
		}
		
		//save defaults
		
	}

}
