package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;

import addition.InMemFSMergeThread;
import addition.LocalFSMerger;

import hasim.core.Datum;
import hasim.core.HDD;
import hasim.gui.HMonitor;

public class HReducerTask extends HTask{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HReducerTask.class);

//	double heartBeat;
//	heartBeat= jobtracker.getConfig().getHeartbeat();

	protected InMemFSMergeThread inMemFSMergeThread;
	public void setInMemFSMergeThread(InMemFSMergeThread inMemFSMergeThread) {
		this.inMemFSMergeThread = inMemFSMergeThread;
	}
//	public void setLocalFSMerger(LocalFSMerger localFSMerger) {
//		this.localFSMerger = localFSMerger;
//	}

	protected final LocalFSMerger localFSMerger;
	

	public HReducerTask(String name, HTaskTracker machine,
			HJobTracker jobtracker, HMonitor monitor) throws Exception {
		super(name, machine, jobtracker, monitor);
		
		localFSMerger=new LocalFSMerger(name+"-localFSMerger");
		inMemFSMergeThread=new InMemFSMergeThread(name+"-inMemFSMergeThread");
	}
	public InMemFSMergeThread getInMemFSMergeThread() {
		return inMemFSMergeThread;
	}
	public LocalFSMerger getLocalFSMerger() {
		return localFSMerger;
	}
	public HReducerTask(String name, HTaskTracker machine,
			HJobTracker jobtracker) throws Exception {
		this(name, machine, jobtracker, null);
	}
	
	@Override
	protected void initStat() {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public void stopEntity() {
		
		inMemFSMergeThread.stopEntity();
		localFSMerger.stopEntity();

		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);

		hlog.info("counters: "+ counters);
		hlog.save();
	}
	
	
}
