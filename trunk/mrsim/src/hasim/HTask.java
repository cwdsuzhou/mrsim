package hasim;

import hasim.gui.HMonitor;
import static hasim.Tools.format;
import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_stat;
import eduni.simjava.Sim_system;

public abstract class HTask extends Sim_entity implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HTask.class);

	public enum Status { idle, running, finished, sort, reduce, copying};
	
	final protected HCounter counters = new HCounter();
	final protected HJobTracker jobTracker;
	final protected String location;


	final protected HMonitor monitor;
	final protected HLogger hlog;
	protected Status status = Status.idle;

	final protected Sim_stat stat;

	final protected HTaskTracker taskTracker;
	
	protected HStory story;

	public HTask(String name,HTaskTracker machine, HJobTracker jobtracker,
			HMonitor monitor) {
		super(name);
		this.jobTracker=jobtracker;
		this.taskTracker = machine;
		this.location = machine.jsonMachine.getHostName();
		this.monitor = monitor;
		this.hlog=new HLogger(location+"-mTask-");

		this.stat=new Sim_stat();

		initStat();
	}

	protected void initStat(){
		
	}
	
	@Override
	public HLogger getHlog() {
		// TODO Auto-generated method stub
		return hlog;
	}
	public String toString2() {
		StringBuffer sb = new StringBuffer("[" + get_name());
		sb.append(",\tid:" + get_id());
		// sb.append(",\t jobId:"+job.getId());
		sb.append(",\t Tracker:" + taskTracker.getHostName());
		sb.append(",\t Start:" + counters.get(CTag.START_TIME));
		sb.append(",\t Stop:" + counters.get(CTag.STOP_TIME));
		sb.append(",\t Tracker:" + taskTracker.getHostName());

		return sb.toString();
	}

	
	@Override
	public String toString() {
		return get_name();
	}
	
	public void setStatus(Status status) {
		this.status = status;
		hlog.info("setStatus("+status.name()+")");
	}
	
	double oneStoryStartTime=0;
	
	public void submitStory() {
		assert story != null;
		assert story.getStatus() != Status.finished;
		assert story.getStatus() != Status.idle;
		
		oneStoryStartTime=Sim_system.clock();
		
		hlog.info("submitStory");
		
		setStatus(Status.running);
		story.setStatus(Status.running);
		
		hlog.info("story generate ");
		
		story.generate(this);
		
		sim_schedule(get_id(), 0.0, HTAG.task_start_local.id());

		counters.put(CTag.START_TIME, Sim_system.clock());

	}
	
	public HStory getStory() {
		return story;
	}

	public void setStory(HStory story) {
		this.story = story;
	}

	public HCounter getCounters() {
		return counters;
	}

	public HJobTracker getJobTracker() {
		return jobTracker;
	}

	public String getLocation() {
		return location;
	}

	public HMonitor getMonitor() {
		return monitor;
	}

	public Status getStatus() {
		return status;
	}

	public Sim_stat getStat() {
		return stat;
	}

	public HTaskTracker getTaskTracker() {
		return taskTracker;
	}

	protected void taskStart(){
		hlog.info("task start");
		story.taskStart(this);
	}
	
	protected void taskProcess() {
		hlog.info("start task process");
		story.taskProcess(this);
	}
	
	protected void taskCleanUp() {
		hlog.info("taskCleanUp");				
		story.taskCleanUp(this);
	}
	
	@Override
	public void body() {
		while (Sim_system.running()) {
			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();


//			sim_trace(1, HTAG.toString(tag));
			//if(monitor != null)
			//	monitor.step(Sim_system.clock(), "event:" + HTAG.toString(tag));

			if (tag == HTAG.END_OF_SIMULATION) {
				hlog.info("END_OF_SIMULATION");
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());
				break;
			}

			if (tag == HTAG.task_start_local.id()) {
				assert getStatus() ==Status.running;
				assert story.getStatus()==Status.running;
				
				taskStart();
				taskProcess();
				taskCleanUp();

				story.setStatus(Status.finished);
				setStatus(Status.idle);
				
				{//counting
					double duration=Sim_system.clock()-oneStoryStartTime;
					oneStoryStartTime=0;
					counters.inc(CTag.process_time, duration);
					hlog.info("duration "+ format(duration)+ 
							", total process:"+ format(counters.get(CTag.process_time)));
				}
				continue;
			}

		}
	}
	
	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
		hlog.info("counters: "+ counters);
		hlog.save();
	}
}
