package de.tuda.p2p.tdf.common;

import org.joda.time.DateTime;

public interface TaskLike {

	/**
	 * @return a text representation of all the task's information except input,
	 *         output, log and error
	 */
	public abstract String asString();

	public abstract String getWorker();

	public abstract void setWorker(String worker);

	public abstract String getInput();

	public abstract void setInput(String input);

	public abstract DateTime getRunBefore();

	public abstract void setRunBefore(DateTime runBefore);

	public abstract DateTime getRunAfter();

	public abstract void setRunAfter(DateTime runAfter);

	public abstract Integer getTimeout();

	public abstract void setTimeout(Integer timeout);

	public abstract Integer getWaitAfterSuccess();

	public abstract void setWaitAfterSuccess(Integer waitAfterSuccess);

	public abstract Integer getWaitAfterSetupError();

	public abstract void setWaitAfterSetupError(Integer waitAfterSetupError);

	public abstract Integer getWaitAfterRunError();

	public abstract void setWaitAfterRunError(Integer waitAfterRunError);


}