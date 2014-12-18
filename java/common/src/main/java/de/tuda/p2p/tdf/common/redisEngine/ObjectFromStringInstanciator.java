package de.tuda.p2p.tdf.common.redisEngine;

import org.joda.time.DateTime;

public class ObjectFromStringInstanciator {
	public static Object convertToObject(String objectAsString, Class clasz) {
		Object obj = null;
		if(clasz == Long.class)	{
			obj = new Long(objectAsString);
		}
		else if(clasz == org.joda.time.DateTime.class) {
			obj = DateTime.parse(objectAsString);
		}
		else if(clasz == String.class) {
			obj = objectAsString;
		}
		else if(clasz == Integer.class) {
			obj = new Integer(objectAsString);
		}
		return obj;
	}

}
