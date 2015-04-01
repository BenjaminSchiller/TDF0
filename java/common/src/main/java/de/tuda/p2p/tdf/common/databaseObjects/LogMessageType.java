package de.tuda.p2p.tdf.common.databaseObjects;

public enum LogMessageType {
	  CLIENT_STARTED("cs"),
	  // Client received signal to shut down
	  CLIENT_TERMINATING("ct"),
	  TASKLIST_STARTED("tls"),
	  TASKLIST_ENDED("tle"),
	  TASK_STARTED("tsa"),
	  TASK_STOLEN("tst"),
	  TASK_SUCCESSFUL("tss"),
	  TASK_FAILED("tfa"),
	  TASK_TIMED_OUT("ttt");
	  
	  
	  private String text;

	  LogMessageType(String text) {
	    this.text = text;
	  }

	  public String getText() {
	    return this.text;
	  }

	  public static LogMessageType fromString(String text) {
	    if (text != null) {
	      for (LogMessageType b : LogMessageType.values()) {
	        if (text.equalsIgnoreCase(b.text)) {
	          return b;
	        }
	      }
	    }
	    return null;
	  }
	}