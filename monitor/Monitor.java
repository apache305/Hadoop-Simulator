package my.hadoop.simulator.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.lang.reflect.*;

import org.apache.hadoop.io.Text;

import my.hadoop.simulator.VirtualEnviroment;

public class Monitor {
	
	public VirtualEnviroment myVirtualEnviroment;
	
	public long  mapInRecords;//number of map records;
	public long mapOutRecords;//number of map records;
	public long  realMapInRecords;//number of map records;
	public long realMapOutRecords;//number of map records;
	public long mapInBytes;
	public long mapOutBytes;
	public long realMapInBytes;
	public long realMapOutBytes;
	public double mapIRRatio;//map input output ratio
	public double realMapIRRatio;//map input output ratio
	public long mapRunTime;
	
	

	
	
	public long redInRecords;//number of reduce records;
	public long redOutRecords;//number of reduce records;
	public long realRedInRecords;//number of reduce records;
	public long realRedOutRecords;//number of reduce records;
	public long redInBytes;
	public long redOutBytes;
	public long realRedInBytes;
	public long realRedOutBytes;
	public double redIRRatio;
	public double realRedIRRatio;
	public long redRunTime;
	
	public double spRatio;


	
	
	
	public ArrayList<Text> sampleMapinput=new ArrayList<Text>();
	public HashMap<String,ArrayList<String>> map_output_TT=new HashMap<String,ArrayList<String>>();
	public HashMap<String, ArrayList<Integer>> map_output_TI=new HashMap<String,ArrayList<Integer>>();
	public HashMap<String,String> red_output_TT=new HashMap<String,String>();
	public HashMap<String, Integer> red_output_TI=new HashMap<String,Integer>();
	
	public boolean flag_TT=false;
	public boolean flag_TI=false;
	public boolean red_flag_TT=false;
	public boolean red_flag_TI=false;

	public long overheadTime;

	
	
	public Monitor(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}
	
	public void report()
	{
		System.out.println("MapInRecords\t"+this.mapInRecords);
		System.out.println("MapOutRecords\t"+this.mapOutRecords);
		System.out.println("MapInBytes\t"+this.mapInBytes);
		System.out.println("MapOutBytes\t"+this.mapOutBytes);
		System.out.println("mapIRRatio\t"+this.mapIRRatio);
		System.out.println("mapRunTimeMS\t"+this.mapRunTime);
		
		
		
		
		System.out.println("RedInRecords\t"+this.redInRecords);
		System.out.println("RedOutRecords\t"+this.redOutRecords);
		System.out.println("RedInBytes\t"+this.redInBytes);
		System.out.println("RedOutBytes\t"+this.redOutBytes);
		System.out.println("redIRRatio\t"+this.redIRRatio);
		System.out.println("redRunTimeMS\t"+this.redRunTime);
		
		
		System.out.println("RealMapInBytes\t"+this.realMapInBytes);
		System.out.println("SampleRatio\t"+this.spRatio);
		System.out.println("OverHead\t"+this.overheadTime);
		
		
		
		
	}

	public void mapProfiler() {
		
		this.myVirtualEnviroment.mySampler.getInputSize();
		this.setMapInBytes();
		this.setMapOutBytes();
		this.mapIRRatio=(double)this.mapInBytes/(double)this.mapOutBytes;

		

	}

	public void reduceProfiler() {
		this.setRedInBytes();
		this.setRedOutBytes();
		this.redIRRatio=(double)this.redInBytes/(double)this.redOutBytes;
		
		
	}
	
	public void setRedOutBytes() {
		
		String record="";
		long bytes=0;
		long redRecords=0;
		
		if(this.red_flag_TT)
		{
			for (String  s:this.red_output_TT.keySet())
			{
				redRecords++;
				String v=this.red_output_TT.get(s);
				record=s+"\t"+v;
				bytes+=record.getBytes().length;

				
			}
		}
		else if(this.red_flag_TI)
		{
			for (String  s:this.map_output_TI.keySet())
			{
				redRecords++;
				Integer v=this.red_output_TI.get(s);
				record=s+"\t"+v;
				bytes+=record.getBytes().length;

			}
		
		}
		this.redOutRecords=redRecords;
		this.redOutBytes=bytes;
		
		
	}

	public void setRedInBytes() {
		//it's said that when reduce process the map out,
		//the merger process only make all the same key will be in the same place,
		//the data format in memory and in disk not as "key value,value,value, 
		//instead of "key value\n key value\n"
		this.redInBytes=this.mapOutBytes;
		
		
		

		
	}

	public void setMapInBytes()
	{
		long tmp=0;
		for(Text s : this.sampleMapinput)
		  {
			String ts=s.toString();	
			tmp+=ts.getBytes().length;
		  }
		
		this.mapInBytes=tmp;
		this.spRatio=(double)this.mapInBytes/(double)this.realMapInBytes;
		
	
	}
	public void setMapOutBytes()
	{
		String record="";
		long bytes=0;
		long records=0;
		long redRecords=0;
		
		if(this.flag_TT)
		{
			for (String  s:this.map_output_TT.keySet())
			{
				redRecords++;
				ArrayList<String> s_a=this.map_output_TT.get(s);
				for(String v:s_a)
				{
					record=s+"\t"+v;
					bytes+=record.getBytes().length;
					records++;
					

				}
			}
		}
		else if(this.flag_TI)
		{
			for (String  s:this.map_output_TI.keySet())
			{
				redRecords++;
				ArrayList<Integer> s_a=this.map_output_TI.get(s);
				for(Integer v:s_a)
				{
					record=s+"\t"+v;
					bytes+=record.getBytes().length;
					records++;

				}
			}
		
		}
		this.mapOutBytes=bytes;
		this.mapOutRecords=records;
		this.redInRecords=redRecords;
	}
	public void setRedRecords()
	{
		this.redOutRecords=this.redInRecords;
		this.redInBytes=this.mapOutBytes;


		
	}

	
	
	public static void main(String [] args) throws IOException
	{
//		FileReader fr=new FileReader(new File("E:\\pri\\Hadoop\\100M.log"));
//		BufferedReader br=new BufferedReader(fr);
//		Str ing l="";
//		long num=0;
//		while((l=br.readLine())!=null)
//		{
//			//System.out.println(l);
//			//System.out.println(l.getBytes().length);
//			num+=l.getBytes().length;
//		
//		}
//		System.out.println(num);
//		br.close();
//		fr.close();

		
		
		String s="-rw-r--r--   1 hadoop supergroup       1366 2012-07-16 13:17 /stdinput/1k";
		
		
		//String regEx="([rwx-]{10}/s+[1-]/s+/w+/s+/w+)(/d+)(/s+.*)";
		//s=s.substring(11);
		System.out.println(s);
		String regEx="(\\w+)\\s+(\\w+)\\s+(\\d+)(\\s.*)";
		Pattern pattern=Pattern.compile(regEx);
		Matcher m=pattern.matcher(s);
		System.out.println(m.find());

		System.out.println(m.group(3));

		
		
		
		
	   
	}
	
	
		
		
	
	
	
	
	
	

}
