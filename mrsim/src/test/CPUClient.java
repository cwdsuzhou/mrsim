package test;


import org.apache.log4j.Logger;

import cputest.CPUThreads;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;
import eduni.simjava.distributions.Sim_negexp_obj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;

import javax.swing.plaf.metal.MetalIconFactory.TreeLeafIcon;

import gridsim.GridSim;
import hasim.HTAG;
import hasim.core.CPU;
import hasim.core.Datum;
import hasim.json.JsonCpu;

public class CPUClient extends Sim_entity  {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CPUClient.class);


	public CPUClient(String name, CPU cpu) throws Exception {
		super(name);
		this.cpu = cpu;
	}

	public static void main(String[] args) {
		// test1();
		// if(true)return;
		//		
		try {

			Sim_system.initialise();

			JsonCpu json=new JsonCpu();
			json.setCores(2);
			json.setSpeed(1);
			CPU cpu = new CPU("cpu", json);

			CPU.setDELTA(0.005);

			logger.info(cpu);

			CPUClient user1 = new CPUClient("user_1", cpu);
			Generator g=new Generator("generator", user1);
			//			

			// Fourth step: Starts the simulation

			Sim_system.run();
			logger.info("Finish Simulation");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unwanted errors happen");
		}
	}

	List<Datum> jobs = new ArrayList<Datum>();
	CPU cpu;

	LinkedList<DataHolder> list=new LinkedList<DataHolder>();

	void processOneJob(){
		if(list.size()==0)return;
		
		DataHolder dh=list.removeFirst();
		dh.startTime=Sim_system.clock();
		cpu.work(dh.job, get_id(), HTAG.cpu.id, dh);
	}
	
	@Override
	public void body() {

		final int maxMappers=16;
		
		int numParallelJobs=maxMappers;

		
		while(Sim_system.running()){
			Sim_event ev=new Sim_event();
			
			sim_get_next(ev);
			int tag=ev.get_tag();
			
			
			if(tag == HTAG.END_OF_SIMULATION){
				break;
			}
			
			if(tag == HTAG.add_pending_shuffle.id){
				list.addLast((DataHolder)ev.get_data());
				if(numParallelJobs>0){
					numParallelJobs--;
					processOneJob();
				}
				continue;
			}
			
			if(tag == HTAG.cpu.id){
				DataHolder dh=(DataHolder)ev.get_data();
				dh.stopTime=Sim_system.clock();
				dh.exeTime=dh.stopTime-dh.startTime;
				
				System.out.println(dh);
				
				numParallelJobs++;
				
				while(list.size() >0 && numParallelJobs >0){
					numParallelJobs--;
					processOneJob();
				}
			}
			
		}
	}



}

class Generator extends Sim_entity{
	//4
	Double[] interval={10.87,4.04,9.63,4.04,5.93,9.2,4.56,6.7,4.66,3.95,5.25,1.27,10.36,4.48,3.37,8.21,2.33,3.21,3.53,3.83,7.28,4.14,4.17,1.64,2.35,5.1,5.2,2.98,3.73,3.66,1.91,4.65,0.87,4.8,3.54,6.88,5.71,6.11,2.62,1.03,4.35,6.14,3.68,8.07,7.59,2.16,7.27,0.51,7.38,8.14,1.89,2.99,2.89,2.78,7.01,10.94,2.08,2.23,3.78,5.58,5.01,7.94,8.18,2.57,10.36,1.95,2.43,3.89,3.62,5.87,0.07,7.99,6.01,0.34,2.42,1.24,3.9,2.86,1.15,6.14,7.25,5.08,3.96,5.98,4.81,0.79,1.82,7.86,1.35,3.6,3.91,0.6,3.73,6.29,3.24,5.65,6.32,3.56,2.26,6.55,2.5,4.69,7.87,1.32,8.48};
	
	//Double[] interval={4.04,10.87,9.63,4.04,5.93,4.56,9.2,6.7,3.95,4.66,1.27,5.25,10.36,4.48,3.37,8.21,2.33,3.21,3.53,7.28,3.83,4.17,4.14,1.64,2.35,5.1,5.2,3.73,2.98,1.91,3.66,4.65,0.87,4.8,3.54,6.88,2.62,6.11,4.35,5.71,6.14,1.03,3.68,8.07,2.16,7.59,0.51,7.27,7.38,2.89,2.99,8.14,1.89,2.78,7.01,10.94,2.08,3.78,2.23,5.58,7.94,5.01,2.57,1.95,8.18,3.89,10.36,2.43,7.99,5.87,0.07,3.62,6.01,0.34,3.9,1.24,2.42,1.15,6.14,2.86,7.25,3.96,5.08,4.81,1.82,5.98,0.79,3.6,1.35,3.73,3.91,0.6,7.86,6.32,6.29,3.24,3.56,6.55,5.65,2.26,7.87,4.69,1.32,2.5,8.48};
	Double[] job={10.12,4.62,9.66,10.22,13.29,11.94,6.32,8.74,6.02,0.07,12.96,0.63,7.98,8.56,10.42,7.07,6.48,3.45,9.05,11.68,0.26,10.48,6.18,6.94,8.8,8.19,5.87,6.35,3.06,10.39,8.09,5.38,11.47,9.98,9.25,9.0,14.27,5.04,1.99,15.72,4.22,3.38,11.59,4.12,9.65,6.96,16.57,5.8,12.69,7.71,12.66,3.86,1.57,13.12,10.77,7.59,9.09,13.43,9.69,12.56,12.51,8.99,14.67,10.31,9.14,3.34,7.65,4.18,15.31,8.73,10.41,3.07,7.26,7.39,10.36,9.45,7.83,10.67,4.38,4.36,4.24,7.95,4.73,13.17,6.63,8.35,7.65,16.91,12.43,10.04,11.78,11.76,10.22,10.81,10.18,11.7,5.98,8.94,10.91,4.21,10.79,9.73,5.37,8.81,12.03};
	
	//4
	Double[] submit={10.873,14.912,24.544,28.585,34.514,43.715,48.271,54.971,59.628,63.577,68.828,70.093,80.451,84.930,88.304,96.516,98.850,102.062,105.592,109.422,116.705,120.840,125.013,126.656,129.002,134.104,139.307,142.287,146.015,149.670,151.576,156.229,157.103,161.898,165.437,172.317,178.025,184.130,186.748,187.780,192.131,198.268,201.952,210.019,217.607,219.764,227.038,227.546,234.930,243.066,244.955,247.948,250.833,253.614,260.623,271.564,273.640,275.865,279.649,285.234,290.242,298.185,306.368,308.940,319.302,321.249,323.677,327.569,331.186,337.050,337.115,345.100,351.105,351.442,353.862,355.098,359.000,361.856,363.009,369.148,376.393,381.473,385.435,391.416,396.230,397.017,398.836,406.697,408.045,411.648,415.554,416.151,419.883,426.169,429.406,435.057,441.371,444.931,447.190,453.735,456.234,460.922,468.796,470.116,478.595};

	{
		assert  interval.length == job.length;
	}
	CPUClient client;
	public Generator(String name, CPUClient client) {
		super(name);
		this.client=client;
	}

	@Override
	public void body() {
		for (int i = 0; i < interval.length; i++) {
			submit2(i, interval[i],submit[i], job[i]);
		}
		
		while(Sim_system.running()){
			sim_get_next(new Sim_event());
		}
		
		sim_pause(100000);
//		submit(5555, 5.5, 6.6);
		
		
	}


	void submit2(int id, double interval, double submitTime, double job){
		DataHolder dh=new DataHolder(id, interval, job);
		dh.submitTime=submitTime;
		sim_schedule(client.get_id(), submitTime, HTAG.add_pending_shuffle.id, dh);

	}
	void submit(int id, double interval,double job){
		DataHolder dh=new DataHolder(id, interval, job);

		sim_pause(interval);
		dh.submitTime=Sim_system.clock();
//		System.out.println("submit "+ dh);
		sim_schedule(client.get_id(), 0.0, HTAG.add_pending_shuffle.id, dh);

	}

}


