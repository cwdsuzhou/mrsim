package addition;

import hasim.json.JsonJob;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_system;

public class TestE extends Sim_entity{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(TestE.class);

	public TestE(String name) {
		super(name);
	}
	
	@Override
	public void body() {
		sleep(5);
		E2 e3=new E2("e3");

		while(true){
			sim_schedule(get_id(), 0.0, 44);
			logger.info("ev "+Sim_system.clock());
			sim_process(1);
			sleep(1000);
		}
	}
	
	public static void sleep(int d){
		try {
			Thread.sleep(d);
		} catch (Exception e) {
		}
	}
	public static void main(String[] args) {
		
		JsonJob job=JsonJob.read("data/json/job/job.json", JsonJob.class);
		JsonJob.save("data/json/job/job_out.json", job);
		if(true)return;
		Sim_system.initialise();
		
		
		TestE main=new TestE("main");
		
		E2 e1=new E2("e1");
		E2 e2=new E2("e2");
		
		Sim_system.initialise();
		
	}

}

class E2 extends Sim_entity{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(E2.class);

	public E2(String name) {
		super(name);
	}
	
	@Override
	public void body() {
		for (int i = 0; i < 10; i++) {
			logger.info(getName()+"   ev "+i);
			sim_process(1);
			TestE.sleep(1000);
		}
		logger.info(getName()+ " finish");
	}

	
}
