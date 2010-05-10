package hasim.core;

import org.apache.log4j.Logger;

import hasim.HTAG;

import java.util.LinkedList;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

public class HEngine<E> extends Sim_entity{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HEngine.class);

	final private int cores;
	public HEngine(String name, int cores) {
		super(name);
		this.cores=cores;
	}

	LinkedList<E> jobs=new LinkedList<E>();

	public void submit(E job){
		sim_schedule(get_id(), 0.0, HTAG.engine_add.id(), job);
	}

	@Override
	public void body() {

		while (Sim_system.running()) {

			Sim_event ev=new Sim_event();
			sim_get_next(ev);

			int tag= ev.get_tag();
			if( tag == HTAG.END_OF_SIMULATION){
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());
				break;
			}
			
			if(tag == HTAG.engine_add.id()){
				E job=(E)ev.get_data();
				assert !jobs.contains(job);
				jobs.add(job);
				continue;
			}
			if(tag == HTAG.engine_check.id()){
				E job=(E)ev.get_data();
				
				continue;
			}
			
			if(tag == HTAG.engine_check_return.id()){
				E job=(E)ev.get_data();
				
				continue;
			}
			
		}
	}

}
