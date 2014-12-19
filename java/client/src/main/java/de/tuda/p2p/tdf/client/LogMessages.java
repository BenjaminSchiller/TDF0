package de.tuda.p2p.tdf.client;

public enum LogMessages {
	  CLIENT_STARTED("cs"),
	  CLIENT_TERMINATING("ct"),
	  TASKLIST_STARTED("tls"),
	  TASKLIST_ENDED("tle"),
	  TASK_STARTED("tsa"),
	  TASK_STOLEN("tst"),
	  TASK_SUCCESSFUL("tss"),
	  TASK_FAILED("tfa"),
	  TASK_TIMED_OUT("ttt");
	  
	  
	  private String text;

	  LogMessages(String text) {
	    this.text = text;
	  }

	  public String getText() {
	    return this.text;
	  }

	  public static LogMessages fromString(String text) {
	    if (text != null) {
	      for (LogMessages b : LogMessages.values()) {
	        if (text.equalsIgnoreCase(b.text)) {
	          return b;
	        }
	      }
	    }
	    return null;
	  }
	}