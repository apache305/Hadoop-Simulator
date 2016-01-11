package my.hadoop.simulator;

import java.io.IOException;


import my.hadoop.simulator.monitor.Monitor;
import my.hadoop.simulator.mr.MapExecutor;
import my.hadoop.simulator.mr.ReduceExecutor;
import my.hadoop.simulator.sampler.Sampler;

import org.apache.hadoop.fs.Path;


import org.apache.hadoop.conf.*;

public class VirtualEnviroment {
	
	  public Class jarClass;
	  public Class mapClass;
	  public Class cobClass;
	  public Class redClass;
	  public Class partitionerClass;
	  public Class outkeyClass;
	  public Class outvalClass;
	  public Class mapOutputKeyClass;
	  public Class mapOutputValueClass;
	  public Class inputFormatClass;
	  public Class defaultInputFormatClass;
	  public Class outputFormatClass;
	  public Class defaultOutputFormatClass;

	  
	  public int numReduceTasks;
	  public Path inputPath;
	  public Path outputPath;
	  public String jobName;
	  public Configuration jobConf;
	  

	  
	  
	  public Sampler mySampler;
	  public Monitor myMonitor;
	  public MapExecutor myMapExecutor;
	  public ReduceExecutor myReduceExecutor;
	  
	  
	  
	  
	  public VirtualEnviroment()
	  {
		  this.mySampler=new Sampler(this);
		  this.myMonitor=new Monitor(this);
	  }

	  
	  public void startJob() throws IOException
	  {
		  //sampling
		  //if (this.mySampler.sample()==-1){
			//  throw new IOException();
		  //}
		  
		  
		  long t1=0;
		  long t2=0;
		  
		  t1=System.currentTimeMillis();
		  if (this.mySampler.sample(1000)==-1){
			  throw new IOException();
		  }
		  //get mapExe and mapper;
		  
		  this.myMapExecutor=new MapExecutor(this);
		  this.myMapExecutor.run();	  		  
		  //System.out.println("map done");
		  this.myMonitor.mapProfiler();		  

		  //get reducerExe and reducer;
		  this.myReduceExecutor=new ReduceExecutor(this);	  
		  this.myReduceExecutor.run();	  	  
		  this.myMonitor.reduceProfiler();
		  //System.out.println("reduce done");
		  t2=System.currentTimeMillis();
		   
		  this.myMonitor.overheadTime=(t2-t1);
		  
		  
		  //run
		  this.myMonitor.report();
		  
	  }
	  
	  public void setSamplerRatio(int sampleR)
	  {
		  this.mySampler.sampleRatio=sampleR;
	  
	  }
	  

	  

}
