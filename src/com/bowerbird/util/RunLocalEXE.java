package com.bowerbird.util;

import java.io.InputStream;
import java.util.Date;

public class RunLocalEXE {
	
	public static void runEXE(String comm) {  
		Date dt1=new Date();
	    System.out.println(comm+" is start!"+dt1.getTime());
	       Runtime rn = Runtime.getRuntime();  
	       Process p = null;  
	       try {  
	           p = rn.exec(comm);  
	           p.waitFor();
	       } catch (Exception e) {  
	           System.out.println("Error my exec ");  
	       }  
	       System.out.println("running:"+(new Date().getTime()-dt1.getTime())+"ms");
	    }
	

}
