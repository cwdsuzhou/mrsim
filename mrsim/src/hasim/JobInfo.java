package hasim;

import org.apache.log4j.Logger;

import addition.ReduceCopier;

import hasim.HTask.Status;
import hasim.core.Datum;
import hasim.gui.SimoTree;
import hasim.json.JsonAlgorithm;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;
import hasim.json.JsonDatum;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_system;

public class JobInfo implements HLoggerInterface{
	
	public static enum State {
		FINISH, FINISH_D, MAP, MAP_D, REDUCE, REDUCE_D, SHUFFLE, SHUFFLE_D, START
	}

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JobInfo.class);

	
	final public static double INIT_TIME=9;
	private static AtomicInteger TOTALID = new AtomicInteger();

	private final HCounter counters,mCounter, rCounter;

	final public HLogger hlog;

	final private int id;
	
	final private Sim_entity user;
	private int returnTag=HTAG.NULL.id;

	public Sim_entity getUser() {
		return user;
	}

	

	public int getReturnTag() {
		return returnTag;
	}

	public HCounter getCounters() {
		return counters;
	}

	public HCounter getmCounter() {
		return mCounter;
	}

	public HCounter getrCounter() {
		return rCounter;
	}

	public void setReturnTag(int returnTag) {
		this.returnTag = returnTag;
	}

	private JsonJob job;
	private final String name;
	private String logFile;
	
	public String getJobdir() {
		return logFile;
	}

	public void setLogFile(String jobdir) {
		this.logFile = jobdir;
		this.hlog.setLogFile(jobdir);
		
		for (HStory story : mappersWaiting) {
			story.getHlog().setLogFile(RF.get(logFile, RF.mappers));
		}
		for (HStory story : reducersWaiting) {
			story.getHlog().setLogFile(RF.get(logFile, RF.reducers));
		}
	}

	public List<HStory> mappersFinished = new ArrayList<HStory>();
	public List<HStory> mappersRunning = new ArrayList<HStory>();
	public List<HStory> mappersWaiting = new ArrayList<HStory>();

	public List<HStory> reducersFinished = new ArrayList<HStory>();
	public List<HStory> reducersRunning = new ArrayList<HStory>();
	public List<HStory> reducersWaiting = new ArrayList<HStory>();

	private Status status = Status.idle;
	
	
	public JobInfo(JsonJob job, Sim_entity user) {
		this.user=user;
		this.job = job;
		this.id = TOTALID.incrementAndGet();
		logger.info("create jobinfo id = "+ id);
		this.name = job.getJobName() + "_" + id;

		hlog = new HLogger(name);
		counters= new HCounter();
		mCounter=new HCounter();
		rCounter=new HCounter();
		
		if (job.getData() != null) {
			
			// split it to the number of mappers
			JsonDatum data = job.getData();
			assert data.getReplica().size()>0;
			
			
			int max=job.getNumberOfMappers();
			for (int i = 0; i < max; i++) {
				JsonDatum split = new JsonDatum();
				split.setName(data.getName() +"-split-" + i);
				split.setSize(data.getSize() / max);
				split.setRecords(data.getRecords() / max);
				split.setReplica(new ArrayList<String>(data.getReplica()));
				job.getInputSplits().add(split);
//				hlog.info("one split "+ split);
			}
		}else{
			assert job.getInputSplits() !=null;
			double totalSize=0, totalRecords=0;
			for (JsonDatum split : job.getInputSplits()) {
				totalSize += split.getSize();
				totalRecords+= split.getRecords();
			}
			JsonDatum data=new JsonDatum();
			data.setName("collected-");
			data.setSize(totalSize);
			data.setRecords(totalRecords);
			job.setData(data);
		}

		// create  m stories
		for (int i = 0; i < job.getInputSplits().size(); i++) {

			JsonDatum split = job.getInputSplits().get(i);

			HMapperStory mStory = new HMapperStory(name + "-mStory_" + i, this);
			mStory.setInputSplit(split);
			mappersWaiting.add(mStory);
		}

		// create r stories
		for (int i = 0; i < job.getNumberOfReducers(); i++) {
//			HReducerStory rStory = new HReducerStory(name + "-rStory_" + i,	this);
			ReduceCopier rStory = new ReduceCopier(name + "-rStory_" + i,this);

			reducersWaiting.add(rStory);
		}
		
		hlog.info("JsonJob:\n"+job.getJobName());
		
		
	}

	public JobInfo(String jobFile, Sim_entity user) {
		this(JsonJob.read(jobFile, JsonJob.class), user);
	}

	public JobInfo(String jobFile) {
		this(JsonJob.read(jobFile, JsonJob.class), null);
	}

	public JsonAlgorithm getAlgorithm() {
		return job.getAlgorithm();
	}

	public int getId() {
		return this.id;

	}

	public JsonJob getJob() {
		return job;
	}

	public Status getStatus() {
		return status;
	}


	
	// public void setAlgorithm(JsonAlgorithm algorithm) {
	// this.algorithm = algorithm;
	// }
	// public void setJson(JsonJob json) {
	// this.json = json;
	// }
	public String getName() {
		return job.getJobName() + "-" + id;
	}

	public void setStatus(Status status) {
		this.status = status;
		hlog.info("setStatus(" + status.name() + ")", Sim_system.clock());

	}

	public void startJob() {
		counters.set(CTag.JOB_START_TIME, Sim_system.clock());
	}
	public void stopJob(Sim_entity jtkr) {

		counters.set(CTag.JOB_STOP_TIME, Sim_system.clock()+INIT_TIME);
		
		double duration = counters.get(CTag.JOB_STOP_TIME)-
			counters.get(CTag.JOB_START_TIME);
		
		counters.set(CTag.JOB_TOTAL_TIME,duration);
		
		
		for (HStory story : mappersFinished ) {
			mCounter.addAll(story.getCounters());
		}
		for (HStory story : reducersFinished) {
			rCounter.addAll(story.getCounters());
		}
		
		counters.addAll(mCounter);
		counters.addAll(rCounter);
		
		
		hlog.infoCounter("\nMap Counters:",mCounter);
		hlog.infoCounter("\nReduce Counters:",rCounter);
		
		hlog.info("av mappers: \t"+ 
				mCounter.get(CTag.DURATION)/job.getNumberOfMappers());
		if(job.getNumberOfReducers()>0)
			hlog.info("av reducers: \t"+ 
				rCounter.get(CTag.DURATION)/job.getNumberOfReducers());
		hlog.infoCounter("\nJob Counters:",counters);
		
		System.out.println("===========================================================================================================================");
		System.out.println("job "+ logFile);
		System.out.println("\nMap Counters:"+mCounter);
		System.out.println("\nReduce Counters:"+rCounter);
		System.out.println("av mappers: "+ 
				mCounter.get(CTag.DURATION)/job.getNumberOfMappers());
		if(job.getNumberOfReducers()>0)
			System.out.println("av reducers: "+ 
				rCounter.get(CTag.DURATION)/job.getNumberOfReducers());
		System.out.println("\nJob Counters:"+counters);
		
		save();
		
		if(user != null){
			logger.info("notify job back");
			jtkr.sim_schedule(user.get_id(), 0.0, returnTag, this);
		}else{
		
			logger.debug("check user in jobinfo");
		}
	}
	
	public void save(){
			getHlog().save();
			for (HStory story : mappersFinished) {
				story.getHlog().save();
			}
			for (HStory story : reducersFinished) {
				story.getHlog().save();
			}
			
			for (HStory story : mappersRunning) {
				story.getHlog().save();
			}
			for (HStory story : reducersRunning) {
				story.getHlog().save();
			}
			for (HStory story : mappersWaiting) {
				story.getHlog().save();
			}
			for (HStory story : reducersWaiting) {
				story.getHlog().save();
			}

	}

	
	

	@Override
	public String toString() {
		return job.getJobName() + "-" + getId();
	}

	

	@Override
	public HLogger getHlog() {
		return hlog;
	}

	public static void main(String[] args) {
		JobInfo jobinfo=new JobInfo("data/json/job.json", null);
		logger.info(jobinfo.getJob());
	}
}
