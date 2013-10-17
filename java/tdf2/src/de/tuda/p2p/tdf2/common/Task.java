package de.tuda.p2p.tdf2.common;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;


public class Task {

	private RedisHash settings = new RedisHash();
	private String namespace = "";
	private String index = "";
	
	public Task(RedisHash settings){
		this.settings=settings;
	}
	
	public String getWorker(){
		return settings.get(TaskSetting.Worker).toString();
	};
	public String getInput(){
		return settings.get(TaskSetting.Input).toString();
	};
	public DateTime getRunBefore(){
		return DateTime.parse(settings.get(TaskSetting.RunBefore).toString());
	}; 
	public DateTime getRunAfter(){
		return DateTime.parse(settings.get(TaskSetting.RunAfter).toString());
	};
	public DateTime getTimeout(){
		return DateTime.parse(settings.get(TaskSetting.Timeout).toString());
	};
	public Long getWaitAfterSetupError(){
		return Long.valueOf(settings.get(TaskSetting.WaitAfterSetupError).toString());
	}; 
	public Long getWaitAfterRunError(){
		return Long.valueOf(settings.get(TaskSetting.WaitAfterRunError).toString());
	};
	public Long getWaitAfterSuccess(){
		return Long.valueOf(settings.get(TaskSetting.WaitAfterSuccess).toString());
	}; 
	public Long getSession(){
		return Long.valueOf(settings.get(TaskSetting.Session).toString());
	};
	public void setWorker(String worker){
		settings.put(TaskSetting.Worker, worker);
	};
	public void setInput(String input){
		settings.put(TaskSetting.Input, input);
	};
	public void setRunBefore(DateTime runBefore){
		settings.put(TaskSetting.RunBefore, runBefore);
	};
	public void setRunAfter(DateTime runAfter){
		settings.put(TaskSetting.RunAfter, runAfter);
	};
	public void setTimeout(long timeout){
		settings.put(TaskSetting.Timeout, timeout);
	};
	public void setWaitAfterSetupError(long waitAfterSetupError){
		settings.put(TaskSetting.WaitAfterSetupError, waitAfterSetupError);
	};
	public void setWaitAfterRunError(long waitAfterRunError){
		settings.put(TaskSetting.WaitAfterRunError, waitAfterRunError);
	};
	public void setWaitAfterSuccess(long waitAfterSuccess){
		settings.put(TaskSetting.WaitAfterSuccess, waitAfterSuccess);
	};
	public void setSession(long session){
		settings.put(TaskSetting.Session, session);
	};
	
	private String getTaskKey(){
		return "tdf."+getNamespace()+".task."+getIndex();
	}
	public String getIndex() {
		return index;
	}
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public void setIndex(String index) {
		this.index = index;
	}
	
	public void save(Jedis jedis){
		settings.save(jedis, getTaskKey());
	}
}

