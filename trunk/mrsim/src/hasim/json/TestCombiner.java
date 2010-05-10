package hasim.json;

import java.util.Calendar;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_port;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.GridletList;

public class TestCombiner extends GridSim{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(TestCombiner.class);

	Sim_port feed_port;
	public TestCombiner(String name) throws Exception {
		super(name);
		feed_port = new Sim_port("feed_port");
		add_port(feed_port);
	}

	@Override
	public void body() {


		logger.info("TestCombinerID: "+ getId());
		logger.info("entityID: "+ super.get_id());
		
		//sim_schedule(feed_port, 5.0,3, "testststest");
		// a loop to get one Gridlet at one time and sends it to other GridSim
		// entity
		for (int i = 0; i < 9; i++) {
			
		
			String msg="msg_"+i;

			logger.info("send msg_"+i+" to combiner at time "+ GridSim.clock());
			super.send(feed_port, 0.5,3, msg);
			sim_pause(1.00);
			//super.gridSimHold(1);
			

		}

		//super.send(feed_port, 0.33, HTAG.END_OF_SIMULATION);
//		super.shutdownGridStatisticsEntity();
//		super.shutdownUserEntity();
//		super.terminateIOEntities();
		
		
	}

	public static String printEQ(){
		return null;
	}
	public static void main(String[] args) {
		try {
			// First step: Initialize the GridSim package. It should be called
			// before creating any entities. We can't run this example without
			// initializing GridSim first. We will get run-time exception
			// error.
			int num_user = 0; // number of users need to be created
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = true; // mean trace GridSim events

			// list of files or processing names to be excluded from any
			// statistical measures
			String[] exclude_from_file = { "" };
			String[] exclude_from_processing = { "" };

			// the name of a report file to be written. We don't want to write
			// anything here. See other examples of using the ReportWriter
			// class
			String report_name = null;

			// Initialize the GridSim package
			logger.info("Initializing GridSim package");
			GridSim.init(num_user, calendar, trace_flag, exclude_from_file,
					exclude_from_processing, report_name);

			TestCombiner testCombiner=new TestCombiner("testCombiner");
			logger.info("create TestCombiner : "+ testCombiner);
			
			//TODO this code is changed
			
			
			//HPCombiner combiner=new HPCombiner("combiner");
		
//			Sim_system.link_ports(testCombiner.get_name(), testCombiner.feed_port.get_pname(),
//					combiner.get_name(),combiner.mem_port.get_pname());
//			
			// Fourth step: Starts the simulation
			GridSim.startGridSimulation();

			
			logger.info("Finish Simulation");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unwanted errors happen");
		}
	}
}
