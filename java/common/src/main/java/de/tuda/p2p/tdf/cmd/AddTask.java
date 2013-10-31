package de.tuda.p2p.tdf.cmd;

import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;


public class AddTask extends CMD{
	
	public static void main(String[] args){
		
		JSONObject json=new JSONObject();
		
		try {
			json = new JSONObject(getInput(args));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

}
