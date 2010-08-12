package addition;

import org.apache.log4j.Logger;

import hasim.CTag;
import hasim.HCounter;
import hasim.HLogger;
import hasim.HMergeQueue;
import hasim.HReducerStory;
import hasim.HReducerTask;
import hasim.HStory;
import hasim.HTAG;
import hasim.HTask;
import hasim.JobInfo;
import hasim.HTask.Status;
import hasim.core.Datum;
import hasim.core.HDD;
import hasim.json.JsonJob;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;

public class ReduceCopier extends HReducerStory {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ReduceCopier.class);

	public static double compress(double size) {
		return size;
	}

	final public static double INIT_TIME=3.0;
	
	public static double decompress(double size) {
		return size;
	}
	public Lock lock=new ReentrantLock(true);
	
	public final Lock memoryLock=new ReentrantLock(true);
	
	// JsonJob conf;
	final int numMaps;
	ReduceCopier reduceCopier;

	CompressionCodec codec;

	public int getNumMaps() {
		return numMaps;
	}

	enum Phase {
		SHUFFLE
	}

	Phase phase;


	final HCounterValue reduceShuffleBytes; 

	final HCounterValue reduceFileWrittenBytes;
	final HCounterValue reduceSpilledRecords;

	// private HCounterValue reduceCombineOutputCounter;

	// A custom comparator for map output files. Here the ordering is determined
	// by the file's size and path. In case of files with same size and
	// different
	// file paths, the first parameter is considered smaller than the second
	// one.
	// In case of files with same size and path are considered equal.
	private Comparator<Datum> mapOutputFileComparator = new Comparator<Datum>() {
		public int compare(Datum a, Datum b) {
			
			double dif=a.size-b.size;
			if(dif != 0)
				return (int)Math.signum(dif);
			else{
				return a.id()-b.id();
			}
		}
	};

	// A sorted set for keeping a set of map output files on disk

	Lock onDiskLock = new ReentrantLock(true);
	public final SortedSet<Datum> mapOutputFilesOnDisk = new TreeSet<Datum>(
			mapOutputFileComparator);

	/**
	 * our reduce task instance
	 */
//	HTask reduceTask;

	/**
	 * the list of map outputs currently being copied
	 */
	final List<Datum> scheduledCopies;

	/**
	 * the results of dispatched copy attempts
	 */
	final List<Datum> copyResults;

	/**
	 * the number of outputs to copy in parallel
	 */
	final int numCopiers;

	/**
	 * a number that is set to the max #fetches we'd schedule and then pause the
	 * schduling
	 */
	final private int maxInFlight;

	/**
	 * the set of unique hosts from which we are copying
	 */
	private Set<String> uniqueHosts;

	/**
	 * A reference to the RamManager for writing the map outputs to.
	 */

	final protected ShuffleRamManager ramManager;

	public ShuffleRamManager getRamManager() {
		return ramManager;
	}

//	/**
//	 * A reference to the local file system for writing the map outputs to.
//	 */
//	private HDD hdd;
//
//	private HDD rfs;
	/**
	 * Number of files to merge at a time
	 */
	final int ioSortFactor;

	/**
	 * A flag to indicate when to exit localFS merge
	 */
	boolean exitLocalFSMerge = false;




	/**
	 * Maximum memory usage of map outputs to merge from memory into the reduce,
	 * in bytes.
	 */
	private final double maxInMemReduce;

	/**
	 * The set of required map outputs
	 */
	final Set<Datum> copiedMapOutputs = Collections
	.synchronizedSet(new TreeSet<Datum>());

	

	/**
	 * Resettable collector used for combine.
	 */
	// private CombineOutputCollector combineCollector = null;

	/**
	 * The interval for logging in the shuffle
	 */
	private static final double MIN_LOG_TIME = 6.0;

	/**
	 * List of in-memory map-outputs.
	 */
	final List<Datum> mapOutputsFilesInMemory = Collections
	.synchronizedList(new LinkedList<Datum>());

	//	int nextMapOutputCopierId = 0;

	public ReduceCopier(String name, JobInfo jobinfo) {
		super(name, jobinfo);
		JsonJob job = jobinfo.getJob();
		this.numMaps = job.getNumberOfMappers();
		// this.reduceTask = ReduceTask.this;

		this.scheduledCopies = new ArrayList<Datum>(100);
		this.copyResults = new ArrayList<Datum>(100);
		// getInt("mapred.reduce.parallel.copies", 5);
		this.numCopiers = job.getMapReduceParallelCopies();

		// Setup the RamManager
		this.ramManager = new ShuffleRamManager(job,this.numCopiers, hlog);

		this.maxInFlight = 4 * numCopiers;
		// this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);
		HCounterValue combineInputCounter = counters
		.hValue(CTag.COMBINE_INPUT_RECORDS);

		

		this.ioSortFactor = job.getIoSortFactor();
		// the exponential backoff formula
		// backoff (t) = init * base^(t-1)
		// so for max retries we get
		// backoff(1) + .... + backoff(max_fetch_retries) ~ max
		// solving which we get
		// max_fetch_retries ~ log((max * (base - 1) / init) + 1) / log(base)
		// for the default value of max = 300 (5min) we get max_fetch_retries =
		// 6
		// the order is 4,8,16,32,64,128. sum of which is 252 sec = 4.2 min

		// optimizing for the base 2




		// conf.getFloat("mapred.job.reduce.input.buffer.percent", 0f);
		final double maxRedPer = job.getMapredJobReduceInputBufferPercent();

		assert maxRedPer <= 1.0 &&  maxRedPer >= 0.0;

		this.maxInMemReduce = job.getMapredChildJavaOpts() * maxRedPer;

		// hostnames
		this.uniqueHosts = new HashSet<String>();


		//init counters
		reduceShuffleBytes = counters
		.hValue(CTag.SHUFFLE);

		reduceFileWrittenBytes = counters
		.hValue(CTag.FILE_BYTES_WRITTEN);

		reduceSpilledRecords = counters
		.hValue(CTag.SPILLED_RECORDS);


		//reduceCombineOutputCounter =
		// counters.hValue(CTag.COMBINE_OUTPUT_RECORDS);

	}

	Lock schedCopyLock = new ReentrantLock(true);

	synchronized public void addScheduledCopy(Datum datum) {
		hlog.info("add map to copy , datum=" + datum);
		schedCopyLock.lock();
		scheduledCopies.add(datum);
		schedCopyLock.unlock();
	}

	synchronized public Datum removeScheduledCopy() {
		schedCopyLock.lock();
//		hlog.info("remove scheduledCopy "+ scheduledCopies.size());
		if (scheduledCopies.size() == 0)
			return null;
		Datum result = scheduledCopies.remove(0);
		schedCopyLock.unlock();
		return result;
	}

	synchronized public int getScheduledCopisSize() {
		schedCopyLock.lock();
		int result = scheduledCopies.size();
		schedCopyLock.unlock();
		return result;
	}

	private boolean busyEnough(int numInFlight) {
		return numInFlight > maxInFlight;
	}


	// int mapsCopied

	public void process() {

		
		int parallelCopies = numCopiers;

		final double heartbeat = task.getJobTracker().getRack().getHeartbeat();

		hlog.info("parallelCopies=" + parallelCopies + ", heartbeat="
				+ heartbeat + ",numMaps" + numMaps);
		// fetch flight
		int numInFlight = 0, numCopied = 0;
		// start the clock for bandwidth measurement
		double startTime = Sim_system.clock();
		double currentTime = startTime;
		double lastOutputTime = 0;
		int lastLog=Integer.MAX_VALUE;



		while (Sim_system.running()) {

			task.sim_schedule(task.get_id(), heartbeat,
					HTAG.reducer_check_mappers_out.id());

			Sim_event ev = new Sim_event();
			Sim_predicate p=new Sim_type_p(
					HTAG.reducer_check_mappers_out.id(),
					HTAG.shuffle_return.id(),
					HTAG.END_OF_SIMULATION);
			task.sim_get_next(p,ev);
			//			task.sim_get_next(ev);

			int tag = ev.get_tag();
			
			if (tag == HTAG.END_OF_SIMULATION){
				logger.info(task.get_name()+" END_OF_SIMULATION "+ Sim_system.clock());
				return;
			}

			if (tag == HTAG.reducer_check_mappers_out.id()) {


				if (copiedMapOutputs.size() < numMaps) {

					currentTime = Sim_system.clock();
					boolean logNow = false;
					if (currentTime - lastOutputTime > MIN_LOG_TIME &&
							lastLog != copiedMapOutputs.size()) {
						lastOutputTime = currentTime;
						lastLog=copiedMapOutputs.size();
						logNow = true;
						
					}
					if (logNow) {
						String logMsg = " Need another "
						+ (numMaps - copiedMapOutputs.size())
						+ " map output(s) " + "where " + numInFlight
						+ " is already in progress";
						hlog.info(logMsg);
					}
				} else {
					hlog.info("copiedMapOUtputs.size()==numMaps, break");
					break;
				}

				while (parallelCopies > 0 && scheduledCopies.size() > 0) {

					Datum mapOutputLoc = removeScheduledCopy();
					hlog.info("remove scheduledCopy "+ scheduledCopies.size()+
							", parallelCopies="+parallelCopies+
							", pendingRequest="+ramManager.getNumPendingRequests()+
							", percent="+ramManager.getPercentUsed()+
							", percentSize="+ramManager.getPercentSize());

					assert mapOutputLoc != null;

					parallelCopies--;

					hlog.info("header: " + mapOutputLoc.id()
							+ ", compressed len: " + mapOutputLoc.size
							+ ", decompressed len: " + mapOutputLoc.size);

					// We will put a file in memory if it meets certain
					// criteria:
					// 1. The size of the (decompressed) file should be less
					// than 25% of
					// the total inmem fs
					// 2. There is space available in the inmem fs

					// Check if this map-output can be saved in-memory
					boolean shuffleInMemory = ramManager
					.canFitInMemory(mapOutputLoc.size);

					// Shuffle
					if (shuffleInMemory) {
						hlog.info("Shuffling " + mapOutputLoc.size
								+ " bytes (" + mapOutputLoc.size
								+ " raw bytes) " + "into RAM from "
								+ mapOutputLoc.getLocation());

						task.sim_schedule(inMemFSMergeThread.get_id(),
								0.0, HTAG.shuffle.id(), mapOutputLoc);

					} else {
						hlog.info("Shuffling " + mapOutputLoc.size
								+ " bytes (" + mapOutputLoc.size
								+ " raw bytes) " + "into Local-FS from "
								+ mapOutputLoc.getLocation());

						task.sim_schedule(localFSMergerThread.get_id(),
								0.0, HTAG.shuffle.id(), mapOutputLoc);

					}

				}

				continue;
			}


			if (tag == HTAG.shuffle_return.id()) {

				hlog.info("shuffle return mapOutputFilesOnDisk.size():"+mapOutputFilesOnDisk.size());
				Datum cr = (Datum) ev.get_data();

				numCopied++;
				reduceShuffleBytes.inc(cr.size);
				if(! cr.isInMemory()){
					reduceFileWrittenBytes.inc(cr.size);
					reduceSpilledRecords.inc(cr.records);
				}

				double secsSinceStart = Sim_system.clock() - startTime;

				double mbs = (reduceShuffleBytes.get()) / (1024 * 1024);
				double transferRate = mbs / secsSinceStart;

				uniqueHosts.remove(cr.getLocation());
				numInFlight--;

				parallelCopies++;

				if(numCopied ==numMaps){
					
					ramManager.close();
					hlog.info("copier break");
					break;
				}

				continue;

			}


		}

		exitLocalFSMerge = true;
		ramManager.close();

		// Do a merge of in-memory files (if there are any)
		
		
		// Wait for the on-disk merge to complete
		hlog.info("Interleaved on-disk merge complete: "
				+ mapOutputFilesOnDisk.size() + " files left.");
//		Datum dDisk=localFSMergerThread.lastMergeAndJoin("msg");
//		addFileOnDisk(dDisk);

		// wait for an ongoing merge (if it is in flight) to complete
		hlog.info("In-memory merge complete: "
				+ mapOutputsFilesInMemory.size() + " files left.");
//		Datum dRam=inMemFSMergeThread.lastMergeAndJoin("msg");
//		
		
		int numMemDiskSegments=mapOutputsFilesInMemory.size();
		double inMemToDiskBytes=0;
		for (Datum mdatum : mapOutputsFilesInMemory) {
			inMemToDiskBytes+=mdatum.size;
		}
		
		if (mapOutputsFilesInMemory.size() > 0 &&
	              ioSortFactor > mapOutputFilesOnDisk.size()){
			
	          // must spill to disk, but can't retain in-mem for intermediate merge
			
			hlog.info("Merged " + numMemDiskSegments + " segments, " +
	                   inMemToDiskBytes + " bytes to disk to satisfy " +
	                   "reduce memory limit");
			
			Datum outd = HMergeQueue.mergeToHard(ioSortFactor,
					mapOutputsFilesInMemory.size(), task, hlog, task
							.getTaskTracker() , counters,
					mapOutputsFilesInMemory, null);// jobinfo.getCombiner()
			mapOutputsFilesInMemory.clear();
			outd.setInMemory(false);
			addFileOnDisk(outd);
			
		}else{
			  hlog.info("Keeping " + numMemDiskSegments + " segments, " +
	                   inMemToDiskBytes + " bytes in memory for " +
	                   "intermediate, on-disk merge");
			  for (Datum inmemdat : mapOutputsFilesInMemory) {
				inmemdat.setInMemory(true);
				addFileOnDisk(inmemdat);
			}
			
		}
		
		//merge memory and disk interleaved data to mem, for reducing
		Datum outToReduce=HMergeQueue.mergeToMem(ioSortFactor,
				numMemDiskSegments(mapOutputFilesOnDisk),
				task, hlog, task.getTaskTracker(),
				counters, mapOutputFilesOnDisk, null);//jobinfo.getCombiner()
		
		
		/////
		hlog.info("reduce phase:"+outToReduce);
		Datum result=reduce(outToReduce);
		hlog.info("reduce result:"+ result);
		setStatus(Status.finished);

	}

	LocalFSMerger localFSMergerThread = null;
	InMemFSMergeThread inMemFSMergeThread = null;

	public static int numMemDiskSegments(Collection<Datum> segmets){
		int result=0;
		for (Datum d : segmets) {
			if(d.isInMemory())
				result++;
		}
		return result;
	}
	public boolean fetchOutputs() {
		int numInFlight = 0, numCopied = 0;

		//
		//		
		// copiers = new ArrayList<MapOutputCopier>(numCopiers);
		//
		// // start all the copying threads
		// for (int i=0; i < numCopiers; i++) {
		// MapOutputCopier copier = new MapOutputCopier("copier"+i,conf);
		// copiers.add(copier);
		// copier.start(this);
		// }
		//
		//
		//		localFSMergerThread.start(this);
		//		inMemFSMergeThread.start(this);

		// start the clock for bandwidth measurement
		double startTime = Sim_system.clock();
		double currentTime = startTime;
		double lastOutputTime = 0;

		// loop until we get all required outputs
		while (copiedMapOutputs.size() < numMaps) {

			currentTime = Sim_system.clock();
			boolean logNow = false;
			if (currentTime - lastOutputTime > MIN_LOG_TIME) {
				lastOutputTime = currentTime;
				logNow = true;
			}
			if (logNow) {
				hlog.info( " Need another "
						+ (numMaps - copiedMapOutputs.size())
						+ " map output(s) " + "where " + numInFlight
						+ " is already in progress");
			}

			while (numInFlight > 0) {
				hlog.debug( " numInFlight = " + numInFlight);
				// the call to getCopyResult will either
				// 1) return immediately with a null or a valid CopyResult
				// object,
				// or
				// 2) if the numInFlight is above maxInFlight, return with a
				// CopyResult object after getting a notification from a
				// fetcher thread,
				// So, when getCopyResult returns null, we can be sure that
				// we aren't busy enough and we should go and get more
				// mapcompletion
				// events from the tasktracker
				Datum cr = getCopyResult(numInFlight);// wait

				numCopied++;
				reduceShuffleBytes.inc(cr.size);

				double secsSinceStart = Sim_system.clock() - startTime;

				double mbs = (reduceShuffleBytes.get()) / (1024 * 1024);
				double transferRate = mbs / secsSinceStart;

				uniqueHosts.remove(cr.getLocation());
				numInFlight--;
			}
		}

		// all done, inform the copiers to exit

		// copiers are done, exit and notify the waiting merge threads
		synchronized (mapOutputFilesOnDisk) {
			mapOutputFilesOnDisk.notify();
		}

		return copiedMapOutputs.size() == numMaps;
	}

	private Datum getCopyResult(int numInFlight) {

		Sim_predicate p = new Sim_type_p(HTAG.getCopyResult.id());
		Sim_event ev = new Sim_event();

		task.sim_get_next(p, ev);
		Datum result = (Datum) ev.get_data();

		return copyResults.remove(0);

	}






	/**
	 * Save the map taskid whose output we just copied. This function assumes
	 * that it has been synchronized on ReduceTask.this.
	 * 
	 * @param taskId
	 *            map taskid
	 */
	synchronized public void noteCopiedMapOutput(Datum did) {
		copiedMapOutputs.add(did);
		ramManager.setNumRequiredMapOutputs(numMaps - copiedMapOutputs.size());
	}

	@Override
	public void generate(HTask rtask) {
		super.generate(rtask);

		HReducerTask task = (HReducerTask) rtask;
		
		setTask(rtask);
		
		this.localFSMergerThread = task.getLocalFSMerger();
		this.inMemFSMergeThread = task.getInMemFSMergeThread();

		inMemFSMergeThread.newStory(this);
		localFSMergerThread.newStory(this);
	}

	@Override
	public void taskCleanUp(HTask task) {
		super.taskCleanUp(task);
	}

	@Override
	public void taskProcess(HTask rtask) {
		assert rtask==task;
		process();
		setStatus(Status.finished);
	}

	@Override
	public void taskStart(HTask rtask) {
		super.taskStart(rtask);
		rtask.sim_process(INIT_TIME);

	}

	public int getNumCopiers() {
		return numCopiers;
	}

	synchronized public void addFileOnDisk(Datum outmrg) {
		assert outmrg != null;
		hlog.info("addFileOnDisk before size:"+ mapOutputFilesOnDisk.size());
		hlog.info("add "+ outmrg);
		hlog.info("to "+ mapOutputFilesOnDisk);

		lock.lock();
		mapOutputFilesOnDisk.add(outmrg);
		hlog.info("addFileOnDisk after size:"+ mapOutputFilesOnDisk.size());
		lock.unlock();		
	}

}
