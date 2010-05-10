package hasim.core;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import gridsim.GridSim;
import hasim.HTAG;

public class CpuTest extends Sim_entity  {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CpuTest.class);

	public CpuTest(String name, CPU cpu) throws Exception {
		super(name);
		this.cpu = cpu;
	}

	public static void main(String[] args) {
		// test1();
		// if(true)return;
		//		
		try {

			Sim_system.initialise();

			CPU cpu = new CPU("cpu","data/json/cpu.json");
			logger.info(cpu);
			
			CpuTest user1 = new CpuTest("user_1", cpu);
//			CpuTest user2 = new CpuTest("user_2", cpu);
			// CpuUser user3=new CpuTest("user1");
			// CpuUser user4=new CpuTest("user1");
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

	
	@Override
	public void body() {

		CPU.setDELTA(0.1);

		cpu.work(10, this.get_id(), HTAG.test_1.id(), "££");
		cpu.work(5, this.get_id(), HTAG.test_2.id(), "££");
		cpu.work(5, this.get_id(), HTAG.test_1.id(), "££");
		cpu.work(10, this.get_id(), HTAG.test_2.id(), "££");


		for (int i = 0; i <4; i++) {
			Object obj=Datum.collectAny(this, HTAG.test_1.id(), HTAG.test_2.id());
			logger.info("recieve "+ obj +" , at time:"+ Sim_system.clock());
			
		}

	}
}

