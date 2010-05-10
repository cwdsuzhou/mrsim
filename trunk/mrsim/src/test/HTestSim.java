package test;

import hasim.HTAG;
import hasim.core.Datum;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

public class HTestSim {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HTestSim.class);

	public static void main(String[] args) {
		Sim_system.initialise();

		User user=new User("user");
		Resource resource=new Resource("resource");

		Sim_system.run();
	}

}

class User extends Sim_entity{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(User.class);

	public User(String name) {
		super(name);
	}

	@Override
	public void body() {
		Sim_entity resource= Sim_system.get_entity("resource");

		logger.info(Sim_system.clock()+"\t event "+HTAG.test_1+" to resource");
		logger.info(Sim_system.clock()+"\t event "+HTAG.test_2+" to resource");

		Datum a=new Datum("a",1,0);
		Datum b=new Datum("b",1,0);
		Datum c=new Datum("c",1,0);

		a.register(this, HTAG.test_1.id());
		b.register(this, HTAG.test_2.id());

		sim_schedule( resource.get_id(), 10	, HTAG.test_blocking.id(), b);
		sim_schedule( resource.get_id(), 10	, HTAG.test_blocking.id(), a);


		Datum.collectOne(this, a.getDoneTag());
		logger.info(Sim_system.clock()+"\t return  "+ a);

		Datum.collectOne(this, b.getDoneTag());
		logger.info(Sim_system.clock()+"\t return  "+ b);

		while(true){
			sim_schedule( resource.get_id(), 10	, HTAG.test_1.id(), c);
			sim_process(1);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}

class Resource extends Sim_entity{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Resource.class);

	public Resource(String name) {
		super(name);
	}

	@Override
	public void body() {
		while(Sim_system.running()){
			Sim_event ev=new Sim_event();
			sim_get_next(ev);
			int tag=ev.get_tag();
			Datum d=(Datum)ev.get_data();

			if(tag == HTAG.END_OF_SIMULATION)break;

			if(tag == HTAG.test_blocking.id()){

				double delay=d.getName().startsWith("a")? 20:30;
				logger.info(Sim_system.clock()+ "\t "+ d+ ", delay:"+delay);
				for (Sim_entity user : d.getUsers()) {
					user.sim_schedule(user.get_id(), delay, d.getDoneTag());

				}
			}
			if( tag == HTAG.test_1.id()){
				int i=9;
			}
		}

		logger.info(Sim_system.clock()+ "\t get out of the loop ");
	}

}