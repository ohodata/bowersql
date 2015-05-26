package com.bowerbird.rpc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

import com.bowerbird.util.DAOUtil;

public class ServerNodeHandler extends IoHandlerAdapter{ 
	public ServerNodeHandler() { 
	} 
	@Override 
	public void messageReceived(IoSession session, Object message) throws Exception {
		if(message instanceof QueryInfo)
		{
			QueryInfo qi=(QueryInfo)message; 
			String id=qi.getID();
			String db_name=qi.getDBName();
			String error=qi.getError();
			String node_id=qi.getNodeID();
			String exec_info=qi.getExecInfo();
			if(!error.equals(""))
			{
				System.out.println(db_name +" query error:"+error);
				session.close(false);
			}
			else
			{
				System.out.println(node_id+" "+exec_info);
				List<HashMap<String,Object>> data=qi.getData();
				List<String> fields=qi.getFields();
				Date dt1=new Date();
				System.out.println(node_id+" query finish!"+dt1.getTime());
				Connection conn=null;
				try {
					conn=DAOUtil.getConn("h2");
					String sql="insert into "+ServerNodeService.sql_map.get(id).get("ServerNodeTable")+" values (";
					for(int j=0;j<fields.size();j++)
					{
						if(j>0) sql+=",";
						sql+="?";
					}
					sql+=")";
					PreparedStatement stmt = conn.prepareStatement(sql);
					
					String tempValue = ""; 
					int data_num=data.size();
					int field_num=fields.size();
					for(int i=0;i<data_num;i++)       
					{        
						HashMap<String,Object> hm = data.get(i);  
						for(int j=0;j<field_num;j++)
						{
							tempValue=(String)hm.get(fields.get(j));
							 stmt.setString(j+1, tempValue);
						}
						
						stmt.execute();
					}
					Date dt2=new Date();
					System.out.println(node_id+" "+ db_name+" " +data_num+" rows loadTime="+(dt2.getTime()-dt1.getTime()));
				}catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (conn != null) {
						try {
							conn.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
			
			int num=ServerNodeService.query_flag.get(id)-1;
			ServerNodeService.query_flag.put(id, num);
		}
		else if(message instanceof MoveDataInfo)
		{
			MoveDataInfo mdi=(MoveDataInfo)message; 
			String id=mdi.getID();
			String db_name=mdi.getDBName();
			String node_id=mdi.getNodeID();
			System.out.println(node_id+" "+ db_name);
			int num=ServerNodeService.query_flag.get(id)-1;
			ServerNodeService.query_flag.put(id, num);
		}
		
	} 

}
