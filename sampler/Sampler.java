package my.hadoop.simulator.sampler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import my.hadoop.simulator.*;

import org.apache.hadoop.io.Text;

public class Sampler {
	
	public VirtualEnviroment myVirtualEnviroment;
	public int sampleRatio=100;//default in the format of %
	public long sampledLines=1000;
	
	public Sampler(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}
	
	public int sample()
	{

	    String input_path=this.myVirtualEnviroment.inputPath.toString();

	    
	    ProcessBuilder pb=new ProcessBuilder("hadoop","jar","Sampler.jar",input_path);
	    try{
		    Process p=pb.start();
		    BufferedInputStream in =new BufferedInputStream(p.getInputStream());
		    BufferedReader br=new BufferedReader(new InputStreamReader(in));
		    String s;

		    long lineNum=0;
		    long mapInNum=0;
		    while((s=br.readLine())!=null)
		    {
		    	if(lineNum%100 <=this.sampleRatio)
		    	{
		    		//System.out.println(s);
		    		
		    		this.myVirtualEnviroment.myMonitor.sampleMapinput.add(new Text(s));
		    		mapInNum++;
		    	}
		    	lineNum++;
		    }
		    this.myVirtualEnviroment.myMonitor.mapInRecords=mapInNum;
		    this.myVirtualEnviroment.myMonitor.realMapInRecords=lineNum;
		    
		    return 0;

	    }
	    catch(Exception e)
	    {
			  System.out.println("Error in Sampling");
			  return -1;
		}
		  
	}
	public int sample(int records)
	{
		this.sampledLines=records;
		
	    String input_path=this.myVirtualEnviroment.inputPath.toString();

	    
	    ProcessBuilder pb=new ProcessBuilder("hadoop","jar","Sampler.jar",input_path);
	    try{
		    Process p=pb.start();
		    BufferedInputStream in =new BufferedInputStream(p.getInputStream());
		    BufferedReader br=new BufferedReader(new InputStreamReader(in));
		    String s;

		    
		    long mapInNum=0;
		    while((s=br.readLine())!=null  && mapInNum<=records)
		    {

		    		
		    	this.myVirtualEnviroment.myMonitor.sampleMapinput.add(new Text(s));
		    	mapInNum++;

		    }
		    this.myVirtualEnviroment.myMonitor.mapInRecords=mapInNum;
		    //this.myVirtualEnviroment.myMonitor.RealMapInRecords=lineNum;
		    return 0;
	    }
	    catch(Exception e)
	    {
			  System.out.println("Error in Sampling");
			  return -1;
		}
		
	}
	public void getInputSize()
	{
		String input_path=this.myVirtualEnviroment.inputPath.toString();
	
		ProcessBuilder pb=new ProcessBuilder("hadoop","fs","-ls",input_path);
	    try{
		    Process p=pb.start();
		    BufferedInputStream in =new BufferedInputStream(p.getInputStream());
		    BufferedReader br=new BufferedReader(new InputStreamReader(in));
		    String s;
		    long size=0;

		    while((s=br.readLine())!=null )
		    {
		    	if(s.startsWith("-r"))
		    	{
		    		//System.out.println(s);
		    		String regEx="(\\w+)\\s+(\\w+)\\s+(\\d+)(\\s.*)";
		    		Pattern pattern=Pattern.compile(regEx);
		    		Matcher m=pattern.matcher(s);
		    		if(m.find()){
		    			size=Long.parseLong(m.group(3));
		    		}

		    	}
		    }
		    
		    this.myVirtualEnviroment.myMonitor.realMapInBytes=size;
	    }catch(Exception e)
		    {
				  System.out.println("Error in getFileSize");

			}
		
	}
	    
	
    
    

}
