package de.tuda.p2p.tdf.common.redisEngine;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

import argo.jdom.JdomParser;
import argo.jdom.JsonNode;
import argo.jdom.JsonField;
import argo.saj.InvalidSyntaxException;


public abstract class DatabaseHashObject implements DatabaseObject{
	
	private HashMap<String,Object> fields;
	
	private static HashMap<String, Class> objectFieldDeclarations;
	
	private String dbKey = "";
	
	public String getDbKey() {
		return dbKey;
	}

	public void setDbKey(String dbKey) {
		this.dbKey = dbKey;
		setField("dbKey", dbKey);
	}

	public DatabaseHashObject() {
		objectFieldDeclarations = new HashMap<String,Class>();
		this.generateFields();
		this.addFieldDeclaration("dbKey", String.class);
		fields = new HashMap<String,Object>();
	}
	
	public void loadFromDB(Jedis jedis, String dbKey) {
		Map<String,String> map = jedis.hgetAll(dbKey);
		setDbKey(dbKey);
		
		for(String key : map.keySet()) {
			if(objectFieldDeclarations.containsKey(key)) {
				//Class type = objectFieldDeclarations.get(key);
				//fields.put(key, type.cast(ObjectFromStringInstanciator.convertToObject(map.get(key), type)));
				setField(key, map.get(key));
			}
		}
	}
	
	public void loadFromJson(String sJson) throws InvalidSyntaxException {
		this.loadFromJson(DatabaseFactory.parseJson(sJson));

	}
	
	public void loadFromJson(JsonNode jsonTree) throws InvalidSyntaxException{
		for(JsonField field : jsonTree.getFieldList()) {
			String fieldName = field.getName().getText();
			if(!objectFieldDeclarations.keySet().contains(fieldName))
				throw new InvalidSyntaxException("The field " + fieldName + " does not exist in the class " + this.getClass().getName(), 0, 0);
			//Class type = objectFieldDeclarations.get(fieldName);
			//this.fields.put(fieldName, type.cast(ObjectFromStringInstanciator.convertToObject(field.getValue().getText(), type)));
			setField(fieldName, field.getValue().getText());
		}
	}
	
	/**
	 * Call an "addFieldDeclaration" here for every field the object should contain
	 * If you add arbitrary Types here, check that they have a proper toString-Method
	 * that can be reread by ObjectFromString (see same package)
	 */
	protected abstract void generateFields();
	
	protected void addFieldDeclaration(String name, Class type) {
		objectFieldDeclarations.put(name, type);
	}
	
	public String toJson() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		String separator = "";
		for(String key : fields.keySet()) {
			sb.append(separator);
			separator = ",\n";
			sb.append("\"");
			sb.append(key);
			sb.append("\" : \"");
			String value = fields.get(key).toString();
			sb.append(value.replaceAll("\\r?\\n", "\\\\n"));
			sb.append("\"");
		}
		sb.append("}");
		return sb.toString();
	}
	
	public void saveToDB(Jedis jedis, String dbKey) {
		setDbKey(dbKey);

		for(String key : fields.keySet()){
			jedis.hset(dbKey, key, fields.get(key).toString());
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(String key : fields.keySet()) {
			sb.append(key);
			sb.append(": ");
			sb.append(fields.get(key));
		}
		return sb.toString();
	}
	
	public Object getField(String name) {
		return fields.get(name);
	}
	
	/**
	 * Set a field if correct object type is given (use wih care!)
	 * @param name
	 * @param value
	 */
	public void setField(String name, Object value) {
		fields.put(name, value);
	}
	
	/**
	 * Set a field by its given string representation
	 * @param name
	 * @param value
	 */
	
	public void setField(String name, String value) {
		Class type = objectFieldDeclarations.get(name);
		fields.put(name, type.cast(ObjectFromStringInstanciator.convertToObject(value, type)));
	}
	
/*	public void applyDefaults(RedisHash rh) {
		
	}*/
}