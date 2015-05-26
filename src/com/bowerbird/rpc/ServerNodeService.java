package com.bowerbird.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import com.bowerbird.util.DAOUtil;
import com.bowerbird.util.MD5Util;
import com.bowerbird.util.SQLParser;

public class ServerNodeService {
	private static final int PORT = 9123;
	private static List<String> nodeArray=new ArrayList<String>();
	public static HashMap<String,HashMap<String,String>> sql_map=new HashMap<String,HashMap<String,String>>();
	public static HashMap<String,Integer> query_flag=new HashMap<String,Integer>();
	private static HashMap<String,ConnectFuture> connArray=new HashMap<String,ConnectFuture>();
	
	public static List<String> getNodeArray()
	{
		return nodeArray;
	}
	public static HashMap<String,ConnectFuture> getConnArray()
	{
		return connArray;
	}
    public static void main( String[] args ) throws IOException
    {
    	init();
    	//moveData("monetdb","test1");
    	parallelQuery("monetdb","select date,sum(open) from test group by date order by date desc limit 10");
    	//parallelQuery("monetdb","select code,sum(open) from test,test group by code limit 10");
    	//parallelQuery("monetdb","select code,avg(abs(open-close)+1) as a1 from test where date like '%02'   group by code order by code desc limit 10");
    	//parallelQuery("monetdb","select count(*) from test where date='2010-09'");
    	//cf.getSession().write(sql);//发送消息
    	//cf.getSession().write("quit");//发送消息 
    	//cf.getSession().getCloseFuture().awaitUninterruptibly();//等待连接断开 
    	//connector.dispose(); 
    	System.exit(0);
    }
    public static void init()
    {
    	nodeArray.add("DataNode0:10.60.26.248");
    	nodeArray.add("DataNode1:10.60.47.180");
    	//nodeArray.add("DataNode1:127.0.0.1");
    	//nodeArray.add("DataNode1:127.0.0.1");
    	//nodeArray.add("DataNode1:42.121.127.186");
    	//nodeArray.add("DataNode2:42.121.107.242");

    	Date dt=new Date();
    	for(int i=0;i<nodeArray.size();i++)
    	{
    		String[] node=nodeArray.get(i).split(":");
    		if(connArray.get(node[0])==null)
    		{
    			ConnectFuture cf=initConnect(node[1],PORT);
        		connArray.put(node[0], cf);
        		System.out.println(node[0]+" connected");
    		}
    		
    	}
    	Date dt1=new Date();
    	 System.out.println("init:"+(dt1.getTime()-dt.getTime()));
    }
    public static ConnectFuture initConnect(String ip,int port)
    {
    	IoConnector connector=new NioSocketConnector();

    	connector.getFilterChain().addLast( "logger", new LoggingFilter() );
    	connector.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new ObjectSerializationCodecFactory()));

    	connector.setHandler( new ServerNodeHandler() );
    	connector.getSessionConfig().setReadBufferSize( 2048 );
    	connector.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
    	ConnectFuture cf =connector.connect(new InetSocketAddress(ip,port));
    	cf.awaitUninterruptibly();//等待连接创建完成 
    	return cf;
    	
    	
    }
    public static Map<String,Object> parallelQuery(String db_name,String sql)
	{
    	Map<String,Object> result=new HashMap<String,Object>();
    	List<HashMap<String,Object>> data=new ArrayList<HashMap<String,Object>>();
    	String errorString="";
    	
    	Date dt=new Date();
		System.out.println("start time:"+dt.getTime());
		
		HashMap<String,String> map=new HashMap<String,String>();
		 map.put("start_time", String.valueOf(dt.getTime())) ;
        map.put("origin_sql", sql) ;
        String id="TAB_"+MD5Util.MD5(map.get("origin_sql"));
        map.put("id", id);
        
        
        
        SQLParser parser=new SQLParser(map.get("id"),map.get("origin_sql"));
        map.put("DataNodeQuerySQL",parser.getDataNodeQuerySQL());
        map.put("ServerNodeCreateSQL",parser.getServerNodeCreateSQL().get("sql"));
        map.put("ServerNodeTable",parser.getServerNodeCreateSQL().get("tab_name"));
        map.put("ServerNodeQuerySQL",parser.getServerNodeQuerySQL());
        
        sql_map.put(id, map);
        query_flag.put(id, nodeArray.size());
        
        
        Date dt1=new Date();
        System.out.println("sql parser:"+(dt1.getTime()-dt.getTime()));
        Connection conn=DAOUtil.getConn("h2");
		Statement stmt;
		try {
			stmt = conn.createStatement();
			Date dt01=new Date();
			System.out.println("h2 conn:"+(dt01.getTime()-dt1.getTime()));
			ResultSet rs =stmt.executeQuery("select count(*) num from INFORMATION_SCHEMA.TABLES where table_name='"+sql_map.get("ServerNodeTable")+"'");
			while(rs.next())
			{
				String n=rs.getString("num");
				if(!n.equals("0"))
				{
					ServerNodeService.query_flag.put(id, 0);
				}
				else
				{
					stmt.execute(map.get("ServerNodeCreateSQL"));
					Date dt2=new Date();
			        System.out.println("create table:"+(dt2.getTime()-dt01.getTime()));
					for(int i=0;i<nodeArray.size();i++)
					{
						String node_id=nodeArray.get(i).split(":")[0];
						ConnectFuture cf =connArray.get(node_id);
						QueryInfo qi=new QueryInfo(id,map.get("DataNodeQuerySQL"),db_name,node_id);
						cf.getSession().write(qi);
						System.out.println("send time:"+new Date().getTime()+" "+(new Date().getTime()-dt2.getTime()));
					}
				}
				break;
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
 			if (conn != null) {
 				try {
 					conn.close();
 				} catch (Exception e) {
 					e.printStackTrace();
 				}
 			}
 		}
		while(true)
		{
			if(query_flag.get(id)==0)
			{
				data=serverQuery(id);
				break;
			}
		}
		map.put("end_time", String.valueOf(new Date().getTime()));
		long start_time=Long.parseLong(map.get("start_time"));
		long end_time=Long.parseLong(map.get("end_time"));
		System.out.println("end time:"+(end_time-start_time));
		result.put("error", errorString);
		result.put("data", data);
		return result;
	}
	public static List<HashMap<String,Object>> serverQuery(String id)
	{
		Date dt1=new Date();
		List<HashMap<String,Object>> result=new ArrayList<HashMap<String,Object>>();
		 List<HashMap<String,Object>> data_array=new ArrayList<HashMap<String,Object>>();
		 Connection conn=null;
	 		try {
	 			conn=DAOUtil.getConn("h2");
	 			Statement stmt = conn.createStatement();
	 			ResultSet rs =stmt.executeQuery(sql_map.get(id).get("ServerNodeQuerySQL"));
	 			ResultSetMetaData rsmd = rs.getMetaData();  
				int numCols = rsmd.getColumnCount();
				String tempValue = "";   
				String tempLabel = "";  
				while(rs.next())        
				{        
					HashMap<String,Object> hm = new HashMap<String,Object>();            
					for (int i = 1; i <= numCols; i++)               
					{                  
						tempValue = rs.getString(i); 
						tempLabel=rsmd.getColumnLabel(i);
						if(tempValue == null) tempValue = "";   
						System.out.println(tempLabel+"="+tempValue);
						hm.put(tempLabel,tempValue);            
					}
					data_array.add(hm); 
				}
				//stmt.execute("drop table "+sql_map.get("ServerNodeTable"));
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
	 	Date dt=new Date();
	 	System.out.println("server query time:"+(dt.getTime()-dt1.getTime()));
		return result;
	} 
	public static void moveData(String db_name,String tab_name)
	{
    	Date dt=new Date();
		System.out.println("start time:"+dt.getTime());
		
		HashMap<String,String> map=new HashMap<String,String>();
		map.put("start_time", String.valueOf(dt.getTime())) ;
        map.put("origin_sql", tab_name) ;
        String id="TAB_"+MD5Util.MD5(map.get("origin_sql"));
        map.put("id", id);
        
        
        
        query_flag.put(id, nodeArray.size());
        
        for(int i=0;i<nodeArray.size();i++)
		{
			String node_id=nodeArray.get(i).split(":")[0];
			ConnectFuture cf =connArray.get(node_id);
			MoveDataInfo mdi=new MoveDataInfo(id,tab_name,db_name,node_id,nodeArray,0);
			cf.getSession().write(mdi);
			System.out.println("send time:"+new Date().getTime()+" "+(new Date().getTime()-dt.getTime()));
		}
        
		while(true)
		{
			if(query_flag.get(id)==0)
			{
				break;
			}
		}
		
		query_flag.put(id, nodeArray.size());
        
        for(int i=0;i<nodeArray.size();i++)
		{
			String node_id=nodeArray.get(i).split(":")[0];
			ConnectFuture cf =connArray.get(node_id);
			MoveDataInfo mdi=new MoveDataInfo(id,tab_name,db_name,node_id,nodeArray,1);
			cf.getSession().write(mdi);
			System.out.println("send time:"+new Date().getTime()+" "+(new Date().getTime()-dt.getTime()));
		}
        while(true)
		{
			if(query_flag.get(id)==0)
			{
				break;
			}
		}
		map.put("end_time", String.valueOf(new Date().getTime()));
		long start_time=Long.parseLong(map.get("start_time"));
		long end_time=Long.parseLong(map.get("end_time"));
		System.out.println("end time:"+(end_time-start_time));
	}
	

}
