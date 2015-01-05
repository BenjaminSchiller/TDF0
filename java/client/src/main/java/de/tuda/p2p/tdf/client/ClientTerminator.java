package de.tuda.p2p.tdf.client;

public class ClientTerminator extends Thread {
	private TaskExecutor te;
	
	public ClientTerminator(TaskExecutor te) {
		super();
		this.te = te;
	}
	
	@Override
	public void run() {
		System.out.println("Termination signal received. Waiting for task and exiting...");
		te.kill();
	}
}
