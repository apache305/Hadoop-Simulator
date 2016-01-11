package my.hadoop.simulator.mr;


import java.io.IOException;
import java.util.HashMap;

import my.hadoop.simulator.VirtualEnviroment;

public class MapExecutor {
	
	public VirtualEnviroment myVirtualEnviroment;
	private long mapStartTime;
	private long mapEndTime;
	
	public MapExecutor(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}
	
	public <INKEY,INVALUE,OUTKEY,OUTVALUE> void newMapper()
	{
		
		org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
		      (org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
		        CommonTool.newInstance(this.myVirtualEnviroment.mapClass);
		


		      mapper.veSetup(this.myVirtualEnviroment);
		      try {
		    	this.mapStartTime=System.currentTimeMillis();
				mapper.run();
				this.mapEndTime=System.currentTimeMillis();
				
				this.myVirtualEnviroment.myMonitor.mapRunTime=this.mapEndTime-this.mapStartTime;
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println(e);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.out.println(e);
			}
		      
		
		
	}

	
	
	public void run(){
		newMapper();
	}
	

}
