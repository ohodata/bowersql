package com.bowerbird.rpc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("serial")
public class QueryInfo implements Serializable{
	private String id;
	private String sql;
	private String node_id;
	private String db_name;
	private String error;
	private String exec_info;
	private List<HashMap<String,Object>> data;
	private List<String> fields;
    
    public QueryInfo(String id,String sql,String db_name,String node_id){  
        this.id = id;  
        this.sql = sql; 
        this.db_name=db_name;
        this.node_id=node_id;
    }  
      
    public String getID() {  
        return id;  
    }  
    
    public String getSQL() {  
        return sql;  
    } 
    public String getNodeID() {  
        return node_id;  
    }  
    public String getDBName() {  
        return db_name;  
    }
    public String getError()
    {
    	return error;
    }
    public void setError(String error)
    {
    	this.error=error;
    }
    public String getExecInfo()
    {
    	return exec_info;
    }
    public void setExecInfo(String exec_info)
    {
    	this.exec_info=exec_info;
    }
    public List<HashMap<String,Object>> getData() {  
        return data;  
    }
    public void setData(List<HashMap<String,Object>> data) {  
        this.data=data;  
    }
    public List<String> getFields() {  
        return fields;  
    }
    public void setFields(List<String> fields) {  
        this.fields=fields;  
    }
}
