package my.hadoop.simulator.mr;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import my.hadoop.simulator.VirtualEnviroment;

public class ReduceExecutor {
	
	public VirtualEnviroment myVirtualEnviroment;
	private long redStartTime;
	private long redEndTime;
	
	public ReduceExecutor(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}

	public <INKEY,INVALUE,OUTKEY,OUTVALUE> void newReducer()
	{
		
		org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
	      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
	        CommonTool.newInstance(this.myVirtualEnviroment.redClass);

		  reducer.veSetup(this.myVirtualEnviroment);
	      try {
	    	 this.redStartTime=System.currentTimeMillis();
			reducer.run();
			this.redEndTime=System.currentTimeMillis();
			
			this.myVirtualEnviroment.myMonitor.redRunTime=this.redEndTime-this.redStartTime;
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		
	}
	
	public void run()
	{
		newReducer();
	}
}
