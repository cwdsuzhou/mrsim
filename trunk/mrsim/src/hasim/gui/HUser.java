package hasim.gui;

import org.apache.log4j.Logger;

import addition2.JobTest;

import hasim.HJobTracker;
import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HTAG;
import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import eduni.simjava.distributions.Sim_negexp_obj;

public class HUser extends Sim_entity implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HUser.class);

	final HJobTracker tracker;
	final HLogger hlog;
	public HUser(String name, HJobTracker tracker) {
		super(name);
		this.tracker=tracker;
		this.hlog=new HLogger(name);
		logger.info("User created");
	}
	
	public void submitJobTest( JobTestInterface test){
		logger.info("submit new jobt test "+ test);
		sim_schedule(get_id(), 0.0, HTAG.local_jobtest.id(), test);
	}
	
	@Override
	public void body() {
		
		logger.info("start");
		while(Sim_system.running()){
			
			logger.info("enter the loop");
			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();

			logger.info("event:" + HTAG.toString(tag)+" at time:"+ Sim_system.clock());

			if (tag == HTAG.END_OF_SIMULATION) {
				logger.info("receive end of simulation");
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());

				break;
			}
			
			if(tag == HTAG.local_jobtest.id()){
				
				logger.info("got now test ");
				JobTestInterface test=(JobTestInterface)ev.get_data();
				test.submit(this);
				logger.info("finish one test");
				continue;
			}

			
			
		}
		
		assert false;
	}
	
	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
//		hlog.info("counters: "+ counters);
//		hlog.save();
	}
	@Override
	public HLogger getHlog() {
		// TODO Auto-generated method stub
		return hlog;
	}

}
