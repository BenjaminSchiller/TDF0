package de.tuda.p2p.tdf.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.Calendar;

public class Logger {
	
	public static void log(String s){
		try {
			Socket Connection = new Socket("localhost", 1337);
			Calendar date = Calendar.getInstance();
	        BufferedReader is = new BufferedReader(new InputStreamReader(Connection.getInputStream()));
	        PrintStream os = new PrintStream(Connection.getOutputStream());
	        
	        os.print(date.getTimeInMillis()+","+s);
	        
	        Connection.close();os.close();is.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void error(String s){
		log(s);
	}

	public static void debug(String string) {
		log(string);
		
	}

}
