package hasim;

import gridsim.GridSim;
import hasim.CopyObject.Type;
import hasim.core.Datum;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_system;

public class HcopierTest extends Sim_entity{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HcopierTest.class);
	
	final HCopier copier;
	HTopology topology;
	public HcopierTest(String name, HCopier copier, HTopology topology) {
		super(name);
		this.copier=copier;
		this.topology=topology;
	}

	@Override
	public void body() {
		double size=5500000;
		String msg="msg return 1";
//		copier.getTracker("m1").getNetend().
//			sim_msg("m2", size, this.get_id(), HTAG.test_1.id(), msg);
//		topology.sim_msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 2");
//		topology.sim_msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 3");
//		topology.sim_msg("a", "b", 5500000, this.get_id(), HTAG.test_1.id(), "msg 4");
//		topology.sim_msg("c", "d", 10000  , this.get_id(), HTAG.test_1.id(), "msg 5");
//		
		copier.copy("m1", "m2", size, this, HTAG.test_1.id(), msg, Type.mem_mem);
		//topology.sendData("d", "e", 3300000, n3);
		for (int i = 0; i < 20; i++) {
			Object object= Datum.collectOne(this, HTAG.test_1.id());
				logger.info("receive "+ object +", "+Sim_system.clock());
		}
		
		

		this.sim_hold(100000000);
		
		topology.stopSimulation();
	}
}
