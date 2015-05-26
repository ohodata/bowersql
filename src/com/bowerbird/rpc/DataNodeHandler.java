package com.bowerbird.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

import com.bowerbird.ftp.FTPTransfer;
import com.bowerbird.util.DAOUtil;
import com.bowerbird.util.GZipUtils;
import com.bowerbird.util.MD5Util;
import com.bowerbird.util.QuickLZ;
import com.bowerbird.util.RunLocalEXE;

public class DataNodeHandler extends IoHandlerAdapter
{
	public static String TMP_PATH="e:/tmp_data/";
	public static String FTP_PATH="e:/apache-ftpserver-1.0.6/res/home/";
	@Override
	public void exceptionCaught( IoSession session, Throwable cause ) throws Exception
	{
		cause.printStackTrace();
	}

	@Override
	public void messageReceived( IoSession session, Object message ) throws Exception
	{
		if(message instanceof QueryInfo)
		{
			QueryInfo qi = (QueryInfo)message;
			Map<String,Object> result=query(qi.getDBName(),qi.getSQL());
			qi.setData((List<HashMap<String,Object>>)result.get("data"));
			qi.setError((String)result.get("error"));
			qi.setFields((List<String>)result.get("fields"));
			qi.setExecInfo((String)result.get("exec_info"));
			session.write( qi );
			System.out.println("Message written..."+new Date().getTime());
		}
		else if(message instanceof BroadcastInfo)
		{

		}
		else if(message instanceof MoveDataInfo)
		{

			MoveDataInfo mdi = (MoveDataInfo)message;
			List<String> nodeArray=mdi.getNodeArray();
			if(mdi.getFlag()==0)
			{
				String filename=mdi.getID()+"_"+mdi.getNodeID();
				exportData(mdi.getDBName(),mdi.getTabName(),filename);
				String path=TMP_PATH+filename+".csv";
				RunLocalEXE.runEXE("lib/qpress64.exe -fv "+path+" "+path+".qp");
				//QuickLZ.compress(path, 1);

				for(int i=0;i<nodeArray.size();i++)
				{
					String node_ip=nodeArray.get(i).split(":")[1];
					String[] args={"-s","-b",node_ip+":2121","admin","admin",filename+".csv.qp",path+".qp"};
					FTPTransfer.upload(args);
					System.out.println(new Date().getTime()+":"+path+".qp>>"+FTP_PATH+filename+".csv.qp");
				}
			}
			else if(mdi.getFlag()==1)
			{
				int size=nodeArray.size();
				String[] fileNames =new String[size];
				for(int i=0;i<size;i++)
				{
					String node_id=nodeArray.get(i).split(":")[0];
					String filename=mdi.getID()+"_"+node_id;
					RunLocalEXE.runEXE("lib/qpress64.exe -dfv "+FTP_PATH+filename+".csv.qp"+" "+FTP_PATH);
					File file=new File(FTP_PATH+filename+".csv.qp");
					file.delete();
					fileNames[i]=FTP_PATH+filename+".csv";
				}
				String ftp_path=FTP_PATH.replaceAll("/", "\\\\");
				RunLocalEXE.runEXE("cmd /c copy "+ftp_path+mdi.getID()+"_* "+ftp_path+mdi.getID()+".csv");
				RunLocalEXE.runEXE("cmd /c del "+ftp_path+mdi.getID()+"_* ");
				importData(mdi.getDBName(),mdi.getID(),FTP_PATH+mdi.getID()+".csv",mdi.getTabName());


			}

			//
			//RunLocalEXE.runEXE("lib/qpress64.exe -dfv "+path+" "+path+".qp1");
			//QuickLZ.decompress(FTP_PATH+mdi.getID()+"_"+mdi.getNodeID()+".csv.qp");
			session.write( mdi );
			System.out.println("Message written..."+new Date().getTime());
		}

	}


	@Override
	public void sessionIdle( IoSession session, IdleStatus status ) throws Exception
	{
		System.out.println( "IDLE " + session.getIdleCount( status ));
	}
	public Map<String,Object> query(String db_name,String sql){
		Date dt1=new Date();
		System.out.println(db_name +" query is start!"+dt1.getTime());
		Map<String,Object> result=new HashMap<String,Object>();
		List<HashMap<String,Object>> data_array=new ArrayList<HashMap<String,Object>>();
		List<String> field_array=new ArrayList<String>();
		String errorString="";
		String exec_info="";

		Connection conn=null;
		try {
			conn=DAOUtil.getConn(db_name);
			Date dt2=new Date();
			Statement stmt = conn.createStatement();
			ResultSet rs =stmt.executeQuery(sql);
			Date dt3=new Date();
			ResultSetMetaData rsmd = rs.getMetaData();  
			int numCols = rsmd.getColumnCount();
			String tempValue = "";   
			String tempLabel = ""; 
			int flag=0;
			while(rs.next())        
			{        
				HashMap<String,Object> hm = new HashMap<String,Object>();            
				for (int i = 1; i <= numCols; i++)               
				{        
					tempValue = rs.getString(i);  
					tempLabel=rsmd.getColumnLabel(i);
					if(tempValue == null) tempValue = "";    
					hm.put(tempLabel,tempValue);   
					if(flag==0)
					{
						field_array.add(tempLabel);
					}
				}
				flag++;
				data_array.add(hm); 
			}
			Date dt4=new Date();
			exec_info=db_name +" connectTime="+(dt2.getTime()-dt1.getTime())+" queryTime="+(dt3.getTime()-dt2.getTime())+" loadTime="+(dt4.getTime()-dt3.getTime())+" transTime="+dt4.getTime();
			System.out.println(exec_info);
		}catch (Exception e) {
			e.printStackTrace();
			errorString=e.toString();
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
					errorString=e.toString();
				}
			}
		}

		result.put("error", errorString);
		result.put("exec_info", exec_info);
		result.put("data", data_array);
		result.put("fields",field_array);
		return result;
	}
	public void exportData(String db_name,String tab_name,String filename){
		Date dt1=new Date();
		System.out.println(db_name +" export is start!"+dt1.getTime());

		Connection conn=null;
		try {
			String path=TMP_PATH+filename+".csv";
			File file=new File(path);
			if(file.exists())
			{
				file.delete();
			}
			//String path=file.getAbsolutePath().replaceAll("\\", "/");

			String sql="copy select * from "+tab_name+" into '"+path+"' DELIMITERS '|','\n','\"'";
			conn=DAOUtil.getConn(db_name);
			Statement stmt = conn.createStatement();
			stmt.execute(sql);
			System.out.println("running:"+(new Date().getTime()-dt1.getTime())+"ms");

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
	public void importData(String db_name,String tab_name,String path,String origin_tab){
		Date dt1=new Date();
		System.out.println(db_name +" import is start!"+dt1.getTime());

		Connection conn=null;
		try {
			conn=DAOUtil.getConn(db_name);
			Statement stmt = conn.createStatement();
			
			String sql="select count(*) as num from sys._tables a join schemas b on (a.schema_id=b.id) where a.name='"+tab_name.toLowerCase()+"' and b.name='sys'";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int num=rs.getInt("num");
				if(num>0)
				{
					stmt.execute("drop table "+tab_name);
				}
				break;
			}
			
			sql="create table "+tab_name+" as select * from "+origin_tab+" with no data";
			stmt.execute(sql);
			
			sql="copy into "+tab_name+" from '"+path+"' DELIMITERS '|','\n','\"'";
			stmt.execute(sql);
			System.out.println("running:"+(new Date().getTime()-dt1.getTime())+"ms");

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
	
	public void BroadcastMotion(String db_name,String sql)
	{
		Date dt1=new Date();
		System.out.println(db_name +" query is start!"+dt1.getTime());
		Map<String,Object> result=new HashMap<String,Object>();
		List<HashMap<String,Object>> data_array=new ArrayList<HashMap<String,Object>>();
		List<String> field_array=new ArrayList<String>();
		String errorString="";
		String exec_info="";

		Connection conn=null;
		try {
			conn=DAOUtil.getConn(db_name);
			Date dt2=new Date();
			Statement stmt = conn.createStatement();
			ResultSet rs =stmt.executeQuery(sql);
			Date dt3=new Date();
			ResultSetMetaData rsmd = rs.getMetaData();  
			int numCols = rsmd.getColumnCount();
			String tempValue = "";   
			String tempLabel = ""; 
			int flag=0;
			while(rs.next())        
			{        
				HashMap<String,Object> hm = new HashMap<String,Object>();            
				for (int i = 1; i <= numCols; i++)               
				{        
					tempValue = rs.getString(i);  
					tempLabel=rsmd.getColumnLabel(i);
					if(tempValue == null) tempValue = "";    
					hm.put(tempLabel,tempValue);   
					if(flag==0)
					{
						field_array.add(tempLabel);
					}
				}
				flag++;
				data_array.add(hm); 
			}
			Date dt4=new Date();
			exec_info=db_name +" connectTime="+(dt2.getTime()-dt1.getTime())+" queryTime="+(dt3.getTime()-dt2.getTime())+" loadTime="+(dt4.getTime()-dt3.getTime())+" transTime="+dt4.getTime();
			System.out.println(exec_info);
		}catch (Exception e) {
			e.printStackTrace();
			errorString=e.toString();
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
					errorString=e.toString();
				}
			}
		}

		result.put("error", errorString);
		result.put("exec_info", exec_info);
		result.put("data", data_array);
		result.put("fields",field_array);
		List<String> nodeArray=ServerNodeService.getNodeArray();
		HashMap<String,ConnectFuture> connArray=ServerNodeService.getConnArray();
		for(int i=0;i<nodeArray.size();i++)
		{
			String node_id=nodeArray.get(i).split(":")[0];
			ConnectFuture cf =connArray.get(node_id);
			//QueryInfo qi=new QueryInfo(id,map.get("DataNodeQuerySQL"),db_name,node_id);
			//cf.getSession().write(qi);
		}

	}

}
