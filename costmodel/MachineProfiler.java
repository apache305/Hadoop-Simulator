package my.hadoop.simulator.costmodel;

import my.hadoop.simulator.VirtualEnviroment;

public class MachineProfiler {
	
	VirtualEnviroment myVirtualEnviroment;
	
	
	public double hardDiskSeqRead=80;//80MB/s
	public double hardDiskSeqWrite=70;//70MB/s
	public double networkBandWidth=100;//100MB/s
	
	
	
	public MachineProfiler(VirtualEnviroment ve)
	{
		this.myVirtualEnviroment=ve;
	}
	
	
	
	
	

}
