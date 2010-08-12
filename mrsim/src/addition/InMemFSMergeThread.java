package addition;

import org.apache.log4j.Logger;

import bsh.commands.dir;


import hasim.HCopier;
import hasim.HLogger;
import hasim.HMergeQueue;
import hasim.HReducerTask;
import hasim.HStory;
import hasim.HTAG;
import hasim.HTask;
import hasim.CopyObject.Type;
import hasim.core.Datum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;

public class InMemFSMergeThread extends Sim_entity {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(InMemFSMergeThread.class);

	static Random rnd=new Random(0);
	ReduceCopier copier;
	final HLogger hlog;
	final double heartbeat=1.0;
	public AtomicBoolean isMerging=new AtomicBoolean(false);
	List<Datum> pendingShuffles=new ArrayList<Datum>();

	//	private ReduceCopier copier;


	private ShuffleRamManager ramManager;
	HLogger tLog;

	public InMemFSMergeThread(String name) {
		super(name);
		this.hlog=new HLogger(name);
	}

	public void body() {
		hlog.info("start entity");


		while (Sim_system.running()) {
			// if(jobsRunning.size()==0 && jobsWaiting.size()==0)


			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();


			if (tag == HTAG.END_OF_SIMULATION) {
				hlog.info("END_OF_SIMULATION TAG", Sim_system.clock());
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());

				break;
			}

			if( tag == HTAG.START.id()){
				tLog.info("MEMORY "+get_name() + "  Thread waiting: ");
				if(oneStory())
					break;
				else
					continue;
			}

		}
	}

	@SuppressWarnings("unchecked")
	public Datum doInMemMerge() {

		copier.memoryLock.lock();

		isMerging.set(true);

		List<Datum> inMemorySegments=copier.mapOutputsFilesInMemory;
		tLog.info("MERGING "+ inMemorySegments.size());

		if (inMemorySegments.size() == 0) {
			return null;
		}

		//name this output file same as the name of the first file that is 
		//there in the current list of inmem files (this is guaranteed to
		//be absent on the disk currently. So we don't overwrite a prev. 
		//created spill). Also we need to create the output file now since
		//it is not guaranteed that this file will be present after merge
		//is called (we delete empty files as soon as we see them
		//in the merge method)

		//figure out the mapId 

		double mergeOutputSize =  0;
		for (Datum m : inMemorySegments) {
			mergeOutputSize+=m.size;
		}
		//		int noInMemorySegments = inMemorySegments.size();


//		HMergeQueue mrgQueue=new HMergeQueue(inMemorySegments);

		//		Datum outmrg= mrgQueue.mergeMapper(copier.ioSortFactor, inMemorySegments.size(),
		//				this, tLog, copier.getTask().getTaskTracker().getHdd(), copier.getCounters());

		Datum outmrg=HMergeQueue.mergeToHard(copier.ioSortFactor,inMemorySegments.size(), 
				this, tLog, copier.getTask().getTaskTracker() , copier.getCounters(), 
				inMemorySegments,null);// copier.getJobinfo().getCombiner()

		//TODO update spilled records

		//        hdd.write(outmrg)

		

		tLog.info(copier + 
				" Merge of the " + inMemorySegments.size() +
				" files in-memory complete." +
				" Local file is " + outmrg.name+ " of size " + outmrg.size);


		// Note the output of the merge
		isMerging.set(false);

		for (Datum segmemt : inMemorySegments) {
			ramManager.unreserve(segmemt.size);			
		}

		inMemorySegments.clear();

		ramManager.reset();

		assert outmrg != null;

		tLog.info("MERGING DONE "+ inMemorySegments.size());

		copier.memoryLock.unlock();
		return outmrg;
	}

	public ReduceCopier getCopier() {
		return copier;
	}


	public HLogger getHlog() {
		return hlog;
	}

	//	public Datum lastMergeAndJoin(String msg) {
	//		sim_schedule(get_id(), 0.0, HTAG.all_shuffles_finished.id());
	//		Sim_predicate p = new Sim_type_p(HTAG.all_shuffles_finished_return.id());
	//		Sim_event ev = new Sim_event();
	//
	//		sim_wait_for(p, ev);
	//		return (Datum)ev.get_data();
	//	}


	public void newStory(HStory rStory){


		hlog.info("new story:"+ rStory);

		this.copier=(ReduceCopier)rStory;

		this.tLog=copier.getHlog();
		this.ramManager=copier.getRamManager();
		this.pendingShuffles.clear();

		sim_schedule(get_id(), 0.0, HTAG.START.id());
	}

	public boolean oneStory(){
//		sim_schedule(get_id(), heartbeat, HTAG.check_doMerge.id);

		int shuffleCount=1;
		List<Datum> pendingShuffles=new ArrayList<Datum>();
		while (Sim_system.running()) {
			// if(jobsRunning.size()==0 && jobsWaiting.size()==0)


			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();
			//			sim_schedule(get_id(), 1.0, HTAG.check_pending_shuffles.id());


			if (tag == HTAG.END_OF_SIMULATION) {
				hlog.info("MEMORY END_OF_SIMULATION TAG");

				return true;
			}

			if( tag== HTAG.check_doMerge.id){

				
//				sim_schedule(get_id(), heartbeat, HTAG.check_doMerge.id);

			}
			if (tag == HTAG.shuffle.id()){

				
				Datum mapOutputLoc=(Datum)ev.get_data();				

				ramManager.incNumPendingRequests();
				pendingShuffles.add(mapOutputLoc);

				tryMerge();

				
				tryShuffle(pendingShuffles);
				
				
			


			}


			if ( tag == HTAG.shuffle_return.id()){
				
//				tryMerge();
				
				tLog.info("shuffle resutrn: "+ shuffleCount);
				shuffleCount++;

				Datum mapOutput=(Datum)ev.get_data();



				// Close the in-memory file
				ramManager.closeInMemoryFile(mapOutput.size);

				// Note that we successfully copied the map-output
				copier.mapOutputsFilesInMemory.add(mapOutput);

				tLog.info("MEMORY return one in-memory shuffle: "+ mapOutput);
				copier.noteCopiedMapOutput(mapOutput);

				sim_schedule(copier.getTask().get_id(), 0.0, HTAG.shuffle_return.id(), mapOutput);
				
				tryMerge();
				tryShuffle(pendingShuffles);

			}


		}
		return false;
	}


	private void tryShuffle(List<Datum> pendingShuffles){
		for (Iterator<Datum> iterator = pendingShuffles.iterator(); iterator
		.hasNext();) {
			Datum datum = iterator.next();
			if (ramManager.canReserve(datum.size)) {
				shuffleInMemory(datum, datum.size);
				iterator.remove();
				//				ramManager.decNumPendingRequests();
			}else{
				break;
			}

		}
	}
	private void tryMerge(){
		if(! ramManager.waitForDataToMerge() ){
			tLog.info("MEMORY waitforDataToMerge: "+ ramManager.waitForDataToMerge());

			Datum outmrg=doInMemMerge();

			assert outmrg !=null;
			copier.addFileOnDisk(outmrg);


		}
	}

	public void setCopier(ReduceCopier copier) {
		this.copier = copier;
	}

	public void setHlog(HLogger hlog) {
		this.tLog = hlog;
	}



	private void shuffleInMemory(Datum mapOutputLoc, double mapOutputLength) {

		ramManager.decNumPendingRequests();
		// Reserve ram for the map-output
		boolean isReserved = ramManager.reserve(mapOutputLength);

		assert isReserved;

		// Are map-outputs compressed?
		if (copier.getJobinfo().getJob().isUseCompression()) {

		}

		Datum mapOutput = new Datum(mapOutputLoc, true);
		mapOutput.setInMemory(true);

		//		logger.info("Read " + mapOutputLength + " bytes from map-output for "
		//				+ mapOutputLoc.getLocation());

		tLog.info("Read " + mapOutputLength + " bytes from map-output for "
				+ mapOutputLoc.getLocation());

		//		sim_schedule(get_id(), 500+ rnd.nextInt(100), HTAG.shuffle_return.id(), mapOutput);

		HCopier hcopier=copier.getTask().getJobTracker().getCopier();
		hcopier.copy(mapOutput.getLocation(), copier.getTask().getLocation(),
				mapOutputLength, this, HTAG.shuffle_return.id(),
				mapOutput, Type.hard_mem);


	}

	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
		//		hlog.info("counters: "+ counters);
		hlog.save();
	}
}



