package logserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class logserver {
	
	private static void log(String s) throws IOException{
		FileWriter fw = new FileWriter(new File(getlogname()),true);
		fw.write(s+System.lineSeparator());
		fw.close();
		
	}

	private static String getlogname() {
		// TODO Auto-generated method stub
		return "Log"+new SimpleDateFormat("yyyyMMdd-HH").format(Calendar.getInstance().getTime())+".log";
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	    ServerSocket MyService = null;
	    try {
	       MyService = new ServerSocket(1337);
	        }
	        catch (IOException e) {
	           System.out.println(e);
	           System.exit(3);
	        }
	    
	    while(true){
	    	try {
				Socket Connection = MyService.accept();
				Calendar date = Calendar.getInstance();
		        BufferedReader is = new BufferedReader(new InputStreamReader(Connection.getInputStream()));
		        PrintStream os = new PrintStream(Connection.getOutputStream());
				log("("+(date.getTimeInMillis()/1000)+ Connection.getInetAddress().toString()+")"+(is.readLine()));
				Connection.close();os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
		
	}

}
