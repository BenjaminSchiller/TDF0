/**
 * 
 */
package de.tuda.p2p.tdf.common;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;

/**
 * @author georg
 *
 */
public class Namespace {
	private Jedis jedis;
	private RedisHash defaults = new RedisHash();
	private String client;
	private DateTime started;
	private DateTime finished;
	private DateTime runBefore;
	private DateTime runAfter;
	private Long index;
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

	public String getClient() {
		return client;
	}

	void setClient(String client) {

		this.client = client;
	}

	public String getStartedAsString() {
		return getStarted().toString();
	}

	public DateTime getStarted() {
		return started;
	}

	void setStarted(DateTime started) {
		this.started = started;

	}

	void setStarted(String started) {
		this.started = DateTime.parse(started);
	}

	public String getFinishedAsString() {
		return getFinished().toString();
	}

	public DateTime getFinished() {
		return finished;
	}

	void setFinished(DateTime finished) {
		this.finished = finished;
	}

	void setFinished(String finished) {
		this.finished = DateTime.parse(finished);
	}

	public String getRunBeforeAsString() {
		return getRunBefore().toString();
	}

	public DateTime getRunBefore() {
		return runBefore;
	}

	public void setRunBefore(String runBefore) {
		this.runBefore = DateTime.parse(runBefore);

	}

	public void setRunBefore(DateTime runBefore) {
		this.runBefore = runBefore;
	}

	public String getRunAfterAsString() {
		return this.getRunAfter().toString();
	}

	public DateTime getRunAfter() {
		return runAfter;
	}

	public void setRunAfter(String runAfter) {
		this.runAfter = DateTime.parse(runAfter);

	}

	public void setRunAfter(DateTime runAfter) {
		this.runAfter = runAfter;
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

	public Long getIndex() {
		return index;
	}

	public void setIndex(Long index) {
		this.index = index;
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

	public boolean isValid() {
		if (runAfter == null)
			return true;
		return runAfter.isBeforeNow();
	}

	public Long validWaitTime() {
		Long waitTime = runAfter.getMillis() - DateTime.now().getMillis();
		return (waitTime > 0) ? waitTime : 0;
	}

	public boolean isTimedOut() {
		if (started == null || getTimeout() == null)
			return false;

		if (runAfter == null)
			return started.plusMillis(getTimeout()).isBeforeNow();

		return runAfter.plusMillis(getTimeout()).isBeforeNow();

	}

	public boolean isStarted() {
		return (started != null);
	}

	public boolean isFinished() {
		return (getFinished()!=null);
	}

	public void start(String client) {
		setClient(client);
		setStarted(DateTime.now());
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
