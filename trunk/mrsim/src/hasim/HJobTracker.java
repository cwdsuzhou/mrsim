package hasim;

import java.io.IOException;
import java.util.*;

import hasim.HLogger.HLogLevel;
import hasim.HTask.Status;
import hasim.core.CPU;
import hasim.core.HDD;
import hasim.core.NetEnd;
import hasim.gui.*;
import hasim.json.*;

import org.apache.log4j.Logger;

import addition.InMemFSMergeThread;
import addition.LocalFSMerger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;


public class HJobTracker extends Sim_entity implements HLoggerInterface{
	private static final Logger logger = Logger.getLogger(HJobTracker.class);
	
	
	public boolean  copyHardToHard(String from, String to, double size,
			Sim_entity entty) {
		
		return true;
	}

	public void copyHardToMem(String from, String to, double size,
			Sim_entity entity) {

	}

	public static void copyMemToHard(String from, String to, double size,
			Sim_entity entity) {

	}


//	final private JsonConfig config;

	final private HLogger hlog;

	HUser huser;
	public List<JobInfo> jobsFinished = new ArrayList<JobInfo>();
	public List<JobInfo> jobsRunning = new ArrayList<JobInfo>();
	public List<JobInfo> jobsWaiting = new ArrayList<JobInfo>();

	/**
	 * used in the debugger
	 */
	public HMonitor monitor, m = new HMonitor("all");

	public JsonRealRack getRack() {
		return rack;
	}


	final private JsonRealRack rack;

	final private List<HMapperTask> mTasksIdle = new ArrayList<HMapperTask>();
	final private List<HMapperTask> mTasksWorking = new ArrayList<HMapperTask>();

	final private List<HReducerTask> rTasksIdle = new ArrayList<HReducerTask>();
	final private List<HReducerTask> rTasksWorking = new ArrayList<HReducerTask>();

	
	final private SimoTree simotree;
	private SimoTreeCollector collector;
	


	private HCopier copier;
	
	public HCopier getCopier() {
		return copier;
	}

	//	/**
//	 * Show JTree panel in the gui mode
//	 */
//	private SimoTree simoTree;

	/**
	 * list of machines in the cluster
	 */
	final private Map<String, HTaskTracker> taskTrackers = new LinkedHashMap<String, HTaskTracker>();

	
	public Map<String, HTaskTracker> getTaskTrackers() {
		return taskTrackers;
	}


	private HTopology topology;

	public HJobTracker(String name, JsonRealRack jsonTopology, SimoTree simoTree)
	throws Exception {
		super(name);

		if (jsonTopology == null)
			jsonTopology = JsonJob.read("data/json/rack_working.json",
					JsonRealRack.class);

		this.monitor = new HMonitor(name);
		//this.config = JsonJob.read(configFile, JsonConfig.class);
		this.rack = jsonTopology;
		this.topology=new HTopology(this.rack);
		this.simotree=simoTree;
		hlog = new HLogger(name);
		
		HDD.setDELTA(rack.getDeltaHDD());
		CPU.setDELTA(rack.getDeltaCPU());
		NetEnd.setDELTA(rack.getDeltaNEt());
		
		HLogger.setLevel(HLogLevel.valueOf(rack.getHlogLevel()));
	}

	public HJobTracker(String name, String topologyFile, SimoTree simoTree)
			throws Exception {
		this(name, JsonJob.read(topologyFile, JsonRealRack.class), simoTree);
	}

	
	@Override
	public void body() {

		logger.info("start entity");
		final double heartbeat=rack.getHeartbeat();
		sim_schedule(get_id(), heartbeat, HTAG.heartbeat.id());

		//	
		// Sim_event e2 = new Sim_event();
		// sim_get_next(HTAG.START.predicate(),e2);
		//		
		while (Sim_system.running()) {
			// if(jobsRunning.size()==0 && jobsWaiting.size()==0)

			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();
			
			
			/*step using input console*/
//			logger.info(Sim_system.clock()+ "\t"+ HTAG.toString(tag));
//			try {System.in.read();} catch (IOException e) {e.printStackTrace();}
		
//			monitor.step(Sim_system.clock(), "event:" + HTAG.toString(tag));
			if (tag == HTAG.END_OF_SIMULATION) {
				monitor.txt(Sim_system.clock(), "END_OF_SIMULATION TAG");
				hlog.info("END_OF_SIMULATION TAG", Sim_system.clock());
				logger.info("receive end of simulation");

				break;
			}

			if (tag == HTAG.job_tracker_add_job.id()) {

				JobInfo jobinfo = (JobInfo) ev.get_data();
				jobsWaiting.add(jobinfo);
				hlog.info("addLocal Job" + jobinfo.getName());
//				monitor.txt(Sim_system.clock(), "addLocal Job:" + jobinfo);

				collector.collect(HTAG.simotree_add_job, jobinfo);

				continue;
			}

			if( tag == HTAG.releaseTask.id()){
				HTask task=(HTask)ev.get_data();
				if(task instanceof HMapperTask){
					releaseTask((HMapperTask)task);
				}else if (task instanceof HReducerTask){
					releaseTask((HReducerTask)task);
				}else{
					assert false;
				}
			}
			if (tag == HTAG.heartbeat.id()) {
				sim_schedule(get_id(), rack.getHeartbeat(), HTAG.heartbeat
						.id());

//				monitor.log(MObject.jobsWaiting.name(), Sim_system.clock(), ""
//						+ jobsWaiting.size(), false);
//				monitor.log(MObject.jobsRunning.name(), Sim_system.clock(), ""
//						+ jobsRunning.size(), false);
//				monitor.log("jobsFinished", Sim_system.clock(), ""
//						+ jobsFinished.size(), false);

				checkMappersRunning();

				checkReducersRunning();

				startNewMappers(jobsRunning);

				startNewMappers(jobsWaiting);

				startNewReducers(jobsRunning);

				updateJobs();

				continue;
			}
			if (tag == HTAG.NULL.id()) {
				System.out.println("null tag");
				continue;
			}

			
		}

	}

	
	private void checkMappersRunning() {
		// logger.debug("time:" + Sim_system.clock());

		for (Iterator<HMapperTask> iter = mTasksWorking.iterator(); iter
				.hasNext();) {
			HMapperTask task = iter.next();

			if (task.getStatus() == Status.idle) {

				// logger.debug("remove task from working "+ task.get_name()
				// + ", at time:"+Sim_system.clock());

				// mapperTask
				iter.remove();// mTasksWorking.remove(task);
				mTasksIdle.add(task);

				collector.collect(HTAG.simotree_remove_task, task);
//				sim_schedule(collector.get_id(), 0.0,
//						HTAG.simotree_remove_task.id(), task);


				// maperStory
				HStory story = task.getStory();
				moveMStory(story.getJobinfo().mappersRunning, story
						.getJobinfo().mappersFinished, (HMapperStory)story);

				// logger.debug("moveup task story"+ story.getName()
				// + ", at time:"+Sim_system.clock());

				
				JobInfo jobinfo = story.getJobinfo();
				
				if (jobinfo.getJob().getNumberOfReducers()== 0 &&
						jobinfo.mappersFinished.size()==
							jobinfo.getJob().getNumberOfMappers()) {
					jobinfo.setStatus(Status.finished);
					jobinfo.stopJob(this);
					moveJobInfo(jobsRunning, jobsFinished, jobinfo);
					

				}
			}

		}
	}
	
	
	private void releaseTask(HTask task, double delay){
		sim_schedule(get_id(), delay, HTAG.releaseTask.id(), task);
	}
	private void releaseTask(HMapperTask task){
		mTasksWorking.remove(task);// mTasksWorking.remove(task);
		mTasksIdle.add(task);
		collector.collect(HTAG.simotree_remove_task, task);
	}
	private void releaseTask(HReducerTask task){
		rTasksWorking.remove(task);// mTasksWorking.remove(task);
		rTasksIdle.add(task);
		collector.collect(HTAG.simotree_remove_task, task);
	}

	private void checkReducersRunning() {
		// logger.debug("time:" + Sim_system.clock());

		for (Iterator<HReducerTask> iter = rTasksWorking.iterator(); iter
				.hasNext();) {
			HReducerTask task = iter.next();

			if (task.getStatus() == Status.idle) {
				// logger.debug("remove task from working "+ task.get_name()
				// + ", at time:"+Sim_system.clock());
				// reducerTask
				iter.remove();// rTasksWorking.remove(task);
				rTasksIdle.add(task);
				collector.collect(HTAG.simotree_remove_task, task);
//
//				sim_schedule(collector.get_id(), 0.0,
//						HTAG.simotree_remove_task.id(), task);

				// reducersStory
				HStory story = task.getStory();

				moveRStory(story.getJobinfo().reducersRunning, story
						.getJobinfo().reducersFinished, story);

				// logger.debug("moveup task story"+ story.getName()
				// + ", at time:"+Sim_system.clock());

				// check if job completed
				JobInfo jobinfo = story.getJobinfo();
				if (jobinfo.reducersRunning.size() == 0
						&& story.getJobinfo().reducersWaiting.size() == 0) {
					jobinfo.setStatus(Status.finished);
					jobinfo.stopJob(this);
					moveJobInfo(jobsRunning, jobsFinished, jobinfo);

				}
			}
		}

	}

	public static  Map<String, HTaskTracker> createTaskTrackers
		(JsonRealRack rack, HTopology top)throws Exception{
		 Map<String, HTaskTracker> result=new LinkedHashMap<String, HTaskTracker>();
		 for (JsonMachine m : rack.getMachines()) {
				// logger.debug("JsonMachine "+ m.getHostName());
				HTaskTracker machine = new HTaskTracker(m, top.getNetEnd( m.getHostName()),
						null);
				result.put(m.getHostName(), machine);
		 };
		return result;
	}
	private String logFile;
	
	public String getLogFile() {
		return logFile;
	}

	public void setLogFile(String logFile) {
		this.logFile = logFile;
	}

	public void createEntities(String resultdir) throws Exception {
		setLogFile(resultdir);
		
		hlog.setLogFile(RF.get(resultdir,RF.hlogs));
		
		assert topology != null;

		
		logger.info("create topology");
		topology.createTopology();
		
		logger.info("config.Heartbeat:" + rack.getHeartbeat()
				+ ",\tread topology.getName:" + rack.getName());

		// init the network topology
		logger.info("init NetEnd from rack");

		logger.debug("number of machines in rack is :"
				+ rack.getMachines().size());

		for (JsonMachine m : rack.getMachines()) {
			// logger.debug("JsonMachine "+ m.getHostName());
			HTaskTracker taskTracker = new HTaskTracker(m, topology.getNetEnd( m.getHostName()),
					this.monitor);
			taskTracker.setJobTracker(this);
			taskTracker.getHlog().setLogFile(RF.get(resultdir, RF.machines));
			taskTracker.getHdd().getHlog().setLogFile(RF.get(resultdir, RF.machines));
			taskTracker.getCpu().getHlog().setLogFile(RF.get(resultdir, RF.machines));
			taskTracker.getNetend().getHlog().setLogFile(RF.get(resultdir, RF.machines));
			
			taskTrackers.put(m.getHostName(), taskTracker);
			// logger.info("add machine " + m.getHostName());

			// numOfMappersMax +=m.getMaxMapper();
			// numOfReducersMax+=m.getMaxReducer();

			for (int i = 0; i < m.getMaxMapper(); i++) {
				HMapperTask task = new HMapperTask(m.getHostName() + "-map-"
						+ i, taskTracker, this, this.monitor);
				task.getHlog().setLogFile(RF.get(resultdir, RF.hlogs));
				mTasksIdle.add(task);

			}

			for (int i = 0; i < m.getMaxReducer(); i++) {
				HReducerTask task = new HReducerTask(m.getHostName()
						+ "-reduce-" + i, taskTracker, this, this.monitor);
				task.getHlog().setLogFile(RF.get(resultdir, RF.hlogs));
				
				rTasksIdle.add(task);
			}

			// shuffleList(mappersWorking);
			// shuffleList(reducersWoring);

			// numOfMappers=numOfMappersMax;
			// numOfReducers=numOfReducersMax;
		}

		
//
//		if (simoTree != null) {
//			simoTree.addRack("rack 01", taskTrackers);
//			simoTree.addTopology(getTopology());
//		}

		huser = new HUser("huser", this);
		huser.getHlog().setLogFile(RF.get(resultdir, RF.hlogs));

		collector=new SimoTreeCollector("simotree", simotree);
		collector.getHlog().setLogFile(RF.get(resultdir, RF.hlogs));

		copier=new HCopier("copier", taskTrackers);
		copier.getHlog().setLogFile(RF.get(resultdir, RF.hlogs));

		collector.collect(HTAG.simotree_add_rack, taskTrackers);
		collector.collect(HTAG.simotree_add_topology, getTopology());
		collector.collect(HTAG.simotree_add_object, collector);
		collector.collect(HTAG.simotree_add_object, copier);
		
		logger.debug("finish create entities");
	}

	

	public HTopology getNetwork() {
		return getTopology();
	}

//	public SimoTree getSimoTree() {
//		return simoTree;
//	}

	public HTopology getTopology() {
		return topology;
	}
	
	public HUser getHUser(){
		return huser;
	}
//
//	private void localSchedular(List<HMapperStory> allStories) {
//
//		Map<String, List<HMapperTask>> mapTasks = new LinkedHashMap<String, List<HMapperTask>>();
//		List<HMapperStory> remainStories = new ArrayList<HMapperStory>();
//		List<HMapperTask> resultTasks = new ArrayList<HMapperTask>();
//
//		// fill all locations of mapprers tasks
//		for (HMapperTask task : mTasksIdle) {
//			List<HMapperTask> list = mapTasks.get(task.getLocation());
//
//			if (list == null)
//				list = new ArrayList<HMapperTask>();
//
//			list.add(task);
//			mapTasks.put(task.getLocation(), list);
//		}
//
//		// allocate mapperTasks to local slots
//		for (HMapperStory mStory : allStories) {
//			String splitLocation = mStory.getInputSplit().getLocation();
//
//			List<HMapperTask> list = mapTasks.get(splitLocation);
//			if (list == null || list.size() == 0) {
//				// not exists local mapperTask
//				remainStories.add(mStory);
//			} else {
//
//				HMapperTask task = list.remove(0);
//				task.setStory(mStory);
//				resultTasks.add(task);
//			}
//
//		}
//
//		// allocate mappersTask for non local slots
//
//		out: for (List<HMapperTask> list : mapTasks.values()) {
//			if (list == null || list.size() == 0)
//				continue;
//			for (HMapperTask task : list) {
//				if (remainStories.size() == 0)
//					break out;
//				task.setStory(remainStories.remove(0));
//				// task.setStatus(status)
//				resultTasks.add(task);
//			}
//		}
//
//		// start new mappers
//		for (HMapperTask task : resultTasks) {
//
//			assert mTasksIdle.contains(task);
//
//			mTasksIdle.remove(task);
//			mTasksWorking.add(task);
//
//			collector.collect(HTAG.simotree_add_task, task);
//
//
//			// moveup(task.getStory(), TreeIndex.running);
//
//			JobInfo jobinfo = task.getStory().getJobinfo();
//			if (jobinfo.mappersRunning.size() == 0) {
//				// moveup(jobinfo, TreeIndex.running);
//			}
//
//			jobinfo.mappersWaiting.remove(task.getStory());
//			jobinfo.mappersRunning.add(task.getStory());
//
//			task.submitStory();
//
//		}
//	}

	private void moveJobInfo(List<JobInfo> from, List<JobInfo> to,
			JobInfo jobinfo) {
		boolean isRemoved=from.remove(jobinfo);
		assert isRemoved;
		to.add(jobinfo);
		
		collector.collect(HTAG.simotree_moveup_job, jobinfo);

//		sim_schedule(collector.get_id(), 0.0,
//				HTAG.simotree_moveup_job.id(), jobinfo);

	}

	private void moveMStory(List<HStory> from, List<HStory> to,
			HStory story) {
		boolean isRemoved=from.remove(story);
		assert isRemoved;
		to.add(story);
		collector.collect(HTAG.simotree_moveup_map, story);

	}

	private void moveRStory(List<HStory> from, List<HStory> to,
			HStory story) {
		boolean isRemoved=from.remove(story);
		assert isRemoved;
		to.add(story);
		collector.collect(HTAG.simotree_moveup_reduce, story);

//		sim_schedule(collector.get_id(), 0.0,
//				HTAG.simotree_moveup_reduce.id(), story);

//		if (simoTree != null)
//			simoTree.moveUpReducerStory(story);
	}

	
	

	

	private void startNewMappers(List<JobInfo> aJobs) {

//		monitor.log("M Slots", Sim_system.clock(), "" + mTasksIdle.size(),
//				false);
		// ////
		// if(numOfMappers== 0 )return;

		if (mTasksIdle.size() == 0)
			return;

		List<HStory> tempStories = new ArrayList<HStory>();

		// fill all mapperStories
		outFill: for (JobInfo info : aJobs) {
			for (HStory mstory : info.mappersWaiting) {
				tempStories.add(mstory);
				if (tempStories.size() == mTasksIdle.size())
					break outFill;
			}
		}

		// localSchedular(allStories);

		for (HStory story : tempStories) {
			JobInfo jobinfo = story.getJobinfo();

			HMapperTask task = mTasksIdle.remove(0);
			task.setStory(story);

			mTasksWorking.add(task);
			task.hlog.info("start mapper story " + story, Sim_system.clock());
			collector.collect(HTAG.simotree_add_task, task);

//			sim_schedule(collector.get_id(), 0.0,
//					HTAG.simotree_add_task.id(), task);

			story.setStatus(Status.running);
			moveMStory(jobinfo.mappersWaiting, jobinfo.mappersRunning, (HMapperStory)story);

			if (jobinfo.getStatus() == Status.idle) {
				jobinfo.setStatus(Status.running);
				jobinfo.startJob();
				moveJobInfo(jobsWaiting, jobsRunning, jobinfo);
			}

			task.submitStory();
		}

	}

	private void startNewReducers(List<JobInfo> aJobs) {
		// aJobs only jobs running

		// logger.debug("available reduces " + rTasksIdle.size());
		// ////
		// if(numOfReducers== 0 )return;
		if (rTasksIdle.size() == 0)
			return;

		List<HStory> tempStories = new ArrayList<HStory>();

		// fill all mapperStories
		outFill: for (JobInfo info : aJobs) {
			if (info.mappersFinished.size() == 0)
				continue;
			for (HStory mstory : info.reducersWaiting) {
				tempStories.add(mstory);
				if (tempStories.size() == rTasksIdle.size())
					break outFill;
			}
		}

		for (HStory story : tempStories) {
			JobInfo jobinfo = story.getJobinfo();
			HReducerTask task = rTasksIdle.remove(0);
			task.setStory(story);

			rTasksWorking.add(task);
			
			collector.collect(HTAG.simotree_add_task, task);

//			sim_schedule(collector.get_id(), 0.0,
//					HTAG.simotree_add_task.id(), task);

			task.hlog.info("go working", Sim_system.clock());

			story.setStatus(Status.running);

			moveRStory(jobinfo.reducersWaiting, jobinfo.reducersRunning, story);

			task.submitStory();
		}

	}

	public void startSimulation() {
		sim_schedule(get_id(), 0.0, HTAG.heartbeat.id());
		sim_schedule(get_id(), 0.0, HTAG.START.id());

	}

	public void stopSimulation() {
		if (getTopology() != null)
			getTopology().stopSimulation();

		// if(simoTree !=null) simoTree.endSimulation();
		logger.info("send end of simualtion " + Sim_system.clock());

		for (HMapperTask task : mTasksIdle) {
			task.stopEntity();
		}
		for (HMapperTask task : mTasksWorking) {
			task.stopEntity();
		}
		for (HReducerTask task : rTasksIdle) {
			task.stopEntity();
		}
		for (HReducerTask task : rTasksWorking) {
			task.stopEntity();
		}


		for (HTaskTracker machine : taskTrackers.values()) {
			machine.getHdd().stopEntity();
			machine.getCpu().stopEntity();
//			machine.getNetend().stopEntity();
		}

		//huser.stopEntity(); user is to stop entity
		
		collector.stopEntity();

		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);

	}

	
	public JobInfo submitJob(String orgJobFile)  {
		JobInfo result=null;
		try {
			result=submitJob(orgJobFile, new JobInfo(orgJobFile));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public JobInfo submitJob(String orgJobFile, JobInfo jobinfo)   {
		assert jobinfo != null;
		logger.info("submit jobid =  " +jobinfo.getId()+ 
				", user ="+ jobinfo.getUser());


		jobinfo.getHlog().info("submitting file :\n\t"+orgJobFile);
		

		jobinfo.setLogFile(RF.newJobDir(HSimulator.resultDir,
				jobinfo.getName()));
		
		RF.copy(orgJobFile, RF.get(jobinfo.getJobdir(), RF.jsonJob));
		//copy rack file to the job dir
		RF.copy( RF.get(HSimulator.resultDir, RF.jsonRack),
				RF.get(jobinfo.getJobdir(), RF.jsonRack));
		
		RF.makeAllJob(jobinfo.getJobdir());
		
		
		logger.info("submit job " + jobinfo.getId() + " , "
				+ Sim_system.clock());

		monitor.txt(Sim_system.clock(), "add job" + jobinfo.getJob());

//		sim_schedule(get_id(), 0.5, HTAG.null_tag.id(), jobinfo);

		assert get_id()>0;
		assert jobinfo != null;
		sim_schedule(get_id(), 0.0, HTAG.job_tracker_add_job.id(), jobinfo);

		logger.info("sumbint schedulled");
		return jobinfo;
	}

	

	private void updateJobs() {

	}

	@Override
	public HLogger getHlog() {
		return hlog;
	}
}
