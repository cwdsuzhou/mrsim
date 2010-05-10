package hasim;

import hasim.gui.HMonitor;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_stat;


	

public class HMapperTask extends HTask{
//	public enum Phase {
//		MAP, REDUCE, SHUFFLE
//	}


	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HMapperTask.class);

	//	HCombiner combiner;

	


	public HMapperTask(String name, HTaskTracker machine, HJobTracker jobtracker) throws Exception {
		this(name, machine, jobtracker, null);
	}
	
	public HMapperTask(String name, HTaskTracker machine, HJobTracker jobtracker,
			HMonitor monitor) throws Exception {
		super(name, machine, jobtracker, monitor);
	}
	
	protected void initStat(){
		stat.add_measure("arrive", Sim_stat.STATE_BASED, 0);
		set_stat(stat);
	}
	
	
}
