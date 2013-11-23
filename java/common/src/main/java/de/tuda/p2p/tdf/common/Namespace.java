/**
 * 
 */
package de.tuda.p2p.tdf.common;

import java.util.Collection;
import java.util.HashSet;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;

/**
 * @author georg
 *
 */
public class Namespace implements TaskLike {
	private Jedis jedis;
	private RedisHash defaults = new RedisHash();

	private String name;

	public Namespace(Jedis jedis, String name) {
		this.setName(name);
		this.setJedis(jedis);

	}

	private void setJedis(Jedis jedis) {

		this.jedis = jedis;

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

	private String Setkey(){
		return "tdf."+ getName();
	}
	private String HashKey() {
		return "tdf."+ getName() + ".defaults";
	}
	/**
	 * 
	 * @param set
	 *            a Set of tasks to be added
	 * @return the number of Tasks added <=set.size()
	 */
	public Long save(Jedis jedis) {
		
		if(
			jedis==null){
			return -1L;
		}
		defaults.save(jedis, HashKey());
		// save defaults
		return 0L;

	}
	public Long save(){
		return save(jedis);
	}

	public String asString() {
		// TODO Auto-generated method stub
		return getName();
	}

	public String toString(){
		return asString();
	}
	public String getWorker() {
		return defaults.get(TaskSetting.Worker).toString();
	}

	public void setWorker(String worker) {
		defaults.put(TaskSetting.Worker, worker);
	}

	public String getInput() {
		return defaults.get(TaskSetting.Input).toString();
	}

	public void setInput(String input) {
		defaults.put(TaskSetting.Input, input);
	}
	public String getRunBeforeAsString() {
		return getRunBefore().toString();
	}

	public DateTime getRunBefore() {
		return DateTime.parse(defaults.get(TaskSetting.RunBefore).toString());
	}

	public void setRunBefore(String runBefore) {
		defaults.put(TaskSetting.RunBefore, runBefore);

	}

	public void setRunBefore(DateTime runBefore) {
		setRunBefore(runBefore.toString());
	}

	public String getRunAfterAsString() {
		return this.getRunAfter().toString();
	}

	public DateTime getRunAfter() {
		return DateTime.parse(defaults.get(TaskSetting.RunAfter).toString());

	}

	public void setRunAfter(String runAfter) {
		defaults.put(TaskSetting.RunAfter, runAfter);

	}

	public void setRunAfter(DateTime runAfter) {
		setRunAfter(runAfter.toString());
	}
	public Integer getTimeout() {
		return Integer.valueOf(defaults.get(TaskSetting.Timeout).toString());
	}

	public void setTimeout(Integer timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public void setTimeout(String timeout) {
		defaults.put(TaskSetting.Timeout, timeout);
	}

	public Integer getWaitAfterSuccess() {
		return Integer.valueOf(defaults.get(TaskSetting.WaitAfterSuccess).toString());

	}

	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
	}

	public void setWaitAfterSuccess(String waitAfterSuccess) {
		defaults.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);

	}

	public Integer getWaitAfterSetupError() {
		return Integer.valueOf(defaults.get(TaskSetting.WaitAfterSetupError).toString());
 
	}

	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public void setWaitAfterSetupError(String waitAfterSetupError) {
		defaults.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	}

	public Integer getWaitAfterRunError() {
		return Integer.valueOf(defaults.get(TaskSetting.WaitAfterRunError).toString());
		 
	}

	public void setWaitAfterRunError(Integer waitAfterRunError) {
		defaults.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public void setWaitAfterRunError(String waitAfterRunError) {
		defaults.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	}

	public String getSession() {
		return defaults.get(TaskSetting.Session).toString();
	}

	public void setSession(String session) {
		defaults.put(TaskSetting.Session, session);
	}

	public boolean isExpired() {
		if (getRunBefore() == null)
			return false;
		return getRunBefore().isBeforeNow();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void applyDefaults(){
		for (TaskList t : getLists()) t.applyDefaults(defaults);
	}

	private Collection<TaskList> getLists() {
		Collection<TaskList> lists = new HashSet<TaskList>();
		for(String id : getJedis().smembers(Setkey())){
			lists.add(new TaskList(jedis,name,Long.parseLong(id)));
		}
		return lists;
	}

	protected Jedis getJedis() {
		return jedis;
	}

	public long getNewIndex() {
		return jedis.incr("tdf." + name + ".index");
	}
}
