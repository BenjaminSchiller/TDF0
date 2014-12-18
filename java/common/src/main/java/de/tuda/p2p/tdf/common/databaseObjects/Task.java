package de.tuda.p2p.tdf.common.databaseObjects;

import java.io.File;

import redis.clients.jedis.Jedis;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import de.tuda.p2p.tdf.common.redisEngine.DatabaseHashObject;

public class Task extends DatabaseHashObject {
	//Database-Stuff
	
	@Override
	protected void generateFields() {
		this.addFieldDeclaration("namespace", String.class);
		this.addFieldDeclaration("session", String.class);
		this.addFieldDeclaration("worker", String.class);
		this.addFieldDeclaration("client", String.class);
		this.addFieldDeclaration("input", String.class);
		this.addFieldDeclaration("timeout", Long.class);
		this.addFieldDeclaration("waitAfterSetupError", Integer.class);
		this.addFieldDeclaration("waitAfterRunError", Integer.class);
		this.addFieldDeclaration("waitAfterSuccess", Integer.class);
		this.addFieldDeclaration("started", DateTime.class);
		this.addFieldDeclaration("finished", DateTime.class);
		this.addFieldDeclaration("index", Long.class);
		this.addFieldDeclaration("runAfter", DateTime.class);
		this.addFieldDeclaration("runBefore", DateTime.class);
		this.addFieldDeclaration("log", String.class);
		this.addFieldDeclaration("error", String.class);
		this.addFieldDeclaration("output", String.class);
	}
	
	public String getNamespace() {
		return (String) this.getField("namespace");
	}
}
