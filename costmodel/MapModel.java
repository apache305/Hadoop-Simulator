package my.hadoop.simulator.costmodel;

import my.hadoop.simulator.VirtualEnviroment;

public class MapModel {
	
VirtualEnviroment myVirtualEnviroment;
	

	
	
	
	public MapModel(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}
	public double map_cpu()
	{
		return 0;
	}
	public double map_io()
	{
		return 0;
	}
	public double map_net()
	{
		return 0;
	}
	
	
	
	
	
	
	
	
	
	protected double map_1_init()
	{
		return 0;
		
	}
	protected double map_2_read()
	{
		return 0;
	}
	protected double map_3_net()
	{
		return 0;
	}
	protected double map_4_parse()
	{
		return 0;
	}
	protected double map_5_mapper()
	{
		return 0;
	}
	protected double map_6_sort()
	{
		return 0;
	}
	protected double map_7_merge()
	{
		return 0;
	}
	protected double map_8_serial()
	{
		return 0;
	}
	protected double map_9_read()
	{
		return 0;
	}
	protected double map_10_write()
	{
		return 0;
	}
	
	
	
	
}
