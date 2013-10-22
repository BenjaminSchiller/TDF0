package de.tuda.p2p.tdf2.common;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;
import de.tuda.p2p.tdf2.common.interfaces.TaskInterface;

public class Task implements TaskInterface {

	@Override
	public String asString() {
		StringBuilder sb = new StringBuilder();
		sb.append("worker: ").append(getWorker()).append("\n");
		sb.append("client: ").append(getClient()).append("\n");
		sb.append("started: ").append(getStarted()).append("\n");
		sb.append("finished: ").append(getFinished()).append("\n");
		sb.append("runAfter: ").append(getRunAfter()).append("\n");
		sb.append("runBefore: ").append(getRunBefore()).append("\n");
		sb.append("timeout: ").append(getTimeout()).append("\n");
		sb.append("waitAfterSuccess: ").append(getWaitAfterSuccess()).append("\n");
		sb.append("waitAfterSetupError: ").append(getWaitAfterSetupError()).append("\n");
		sb.append("waitAfterRunError: ").append(getWaitAfterRunError()).append("\n");
		sb.append("index: ").append(getIndex()).append("\n");
		sb.append("namespace: ").append(getNamespace()).append("\n");
		sb.append("session: ").append(getSession()).append("\n");
		return sb.toString();
	}

	@Override
	public Long save(Jedis jedis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long save(Jedis jedis, String namespace) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long save(Jedis jedis, String namespace, Long index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getWorker() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorker(String worker) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getInput() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setInput(String input) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLog() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getError() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClient() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStartedAsString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTime getStarted() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFinishedAsString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTime getFinished() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRunBeforeAsString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTime getRunBefore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRunBefore(String runBefore) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setRunBefore(DateTime runBefore) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getRunAfterAsString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTime getRunAfter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRunAfter(String runAfter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setRunAfter(DateTime runAfter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer getTimeout() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setTimeout(Integer timeout) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTimeout(String timeout) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer getWaitAfterSuccess() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWaitAfterSuccess(Integer waitAfterSuccess) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setWaitAfterSuccess(String waitAfterSuccess) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer getWaitAfterSetupError() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWaitAfterSetupError(Integer waitAfterSetupError) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setWaitAfterSetupError(String waitAfterSetupError) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer getWaitAfterRunError() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWaitAfterRunError(Integer waitAfterRunError) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setWaitAfterRunError(String waitAfterRunError) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Long getIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setIndex(Long index) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getNamespace() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setNamespace(String namespace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSession() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSession(String session) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isExpired() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Long validWaitTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isTimedOut() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isStarted() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFinished() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start(String client) {
		// TODO Auto-generated method stub
		
	}

}
