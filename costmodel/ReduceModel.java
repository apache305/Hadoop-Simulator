package my.hadoop.simulator.costmodel;

import my.hadoop.simulator.VirtualEnviroment;

public class ReduceModel {
	
	VirtualEnviroment myVirtualEnviroment;
	

	
	
	
	public ReduceModel(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}
	
	
	
	public double reduce_cpu()
	{
		return 0;
	}
	public double reduce_io()
	{
		return 0;
	}
	public double reduce_net()
	{
		return 0;
	}
	
	
	protected double reduce_1_init()
	{
		return 0;
		
	}
	protected double reduce_2_read()
	{
		return 0;
	}
	protected double reduce_3_net()
	{
		return 0;
	}
	protected double reduce_4_merge()
	{
		return 0;
	}
	protected double reduce_5_serial()
	{
		return 0;
	}
	protected double reduce_6_io()
	{
		return 0;
	}
	protected double reduce_7_parse()
	{
		return 0;
	}
	protected double reduce_8_reducer()
	{
		return 0;
	}
	protected double reduce_9_net()
	{
		return 0;
	}
	protected double reduce_10_write()
	{
		return 0;
	}


}
