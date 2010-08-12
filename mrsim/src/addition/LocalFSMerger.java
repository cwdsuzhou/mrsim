package addition;

import org.apache.log4j.Logger;

import gridsim.datagrid.storage.HarddriveStorage;
import hasim.HCopier;
import hasim.HLogger;
import hasim.HMergeQueue;
import hasim.HReducerTask;
import hasim.HStory;
import hasim.HTAG;
import hasim.HTask;
import hasim.JobInfo;
import hasim.CopyObject.Type;
import hasim.core.Datum;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;
import eduni.simjava.distributions.Sim_random_obj;

/**
 * Starts merging the local copy (on disk) of the map's output so that most of
 * the reducer's input is sorted i.e overlapping shuffle and merge phases.
 */
public class LocalFSMerger extends Sim_entity {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(LocalFSMerger.class);

	public static double getChecksumLength(double approxOutputSize,
			int bytesPerSum) {
		// the checksum length is equal to size passed divided by bytesPerSum +
		// bytes written in the beginning of the checksum file.
		return ((approxOutputSize + bytesPerSum - 1) / bytesPerSum) * 4 + 8;
	}

	HLogger tLog;
	final HLogger hlog;
	ReduceCopier copier;

	// private HReducerTask task;
	public LocalFSMerger(String name) {
		super(name);
		this.hlog=new HLogger(name);
	}


	public void newStory(HStory rStory) {
		hlog.info("newStory "+ rStory);
		this.copier = (ReduceCopier) rStory;
		this.tLog = rStory.getHlog();
		
		sim_schedule(get_id(), 0.0, HTAG.START.id());

	}
	
	public boolean oneStory(){
		while (Sim_system.running()) {
			// if(jobsRunning.size()==0 && jobsWaiting.size()==0)

			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();

			if (tag == HTAG.END_OF_SIMULATION) {
				hlog.info("DSK-Merger END_OF_SIMULATION TAG", Sim_system.clock());
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());

				return true;
			}
			
			if (tag == HTAG.shuffle.id()) {
				tLog.info("DSK-Merger "+ HTAG.toString(tag));

				Datum mapOutputLoc = (Datum) ev.get_data();
				shuffleToDisk(mapOutputLoc, mapOutputLoc.size);

				continue;
			}

//			if (tag == HTAG.all_shuffles_finished.id()) {
//				
//				tLog.info("DSK-Merger "+ HTAG.toString(tag));
//
//				Datum rDatum=doInDiskMerg();
//
//				sim_schedule(get_id(), 0.0, HTAG.all_shuffles_finished_return
//						.id(), rDatum);
//				continue;
//			}

			if (tag == HTAG.shuffle_return.id()) {
				tLog.info("DSK-Merger "+ HTAG.toString(tag));
				
				
				Datum mapOutput = (Datum) ev.get_data();

				copier.noteCopiedMapOutput(mapOutput);

//				copier.lock.lock();
				
				copier.addFileOnDisk(mapOutput);
				
				if (copier.mapOutputFilesOnDisk.size() >= (2 * copier.ioSortFactor - 1)) {
					Datum outmrg=doInDiskMerg();
					copier.addFileOnDisk(outmrg);	
					tLog.info("DSK-Merger "+get_name() + " Thread waiting: ");
				}
//				copier.lock.unlock();
				
				sim_schedule(copier.getTask().get_id(), 0.0, HTAG.shuffle_return.id(), mapOutput);

				continue;
			}
			
			
			
		}
		return false;
	}

	@Override
	public void body() {

		while (Sim_system.running()) {

			Sim_event ev = new Sim_event();
			sim_get_next(ev);
			int tag = ev.get_tag();

			if (tag == HTAG.END_OF_SIMULATION) {
				hlog.info("END_OF_SIMULATION TAG", Sim_system.clock());

				break;
			}
			if( tag == HTAG.START.id()){
				tLog.info(get_name() + "  Thread waiting: ");
				if(oneStory())
					break;
				else
					continue;
			}

		}
	}

	public Datum doInDiskMerg() {
		isMerging.set(true);
		
		copier.lock.lock();
		

		List<Datum> mapFiles = new ArrayList<Datum>();
		double approxOutputSize = 0;
		int bytesPerSum = copier.getJobinfo().getJob().getInt("io.bytes.per.checksum", 512);
		tLog.info("DSK-Merger "+copier + "We have  " + copier.mapOutputFilesOnDisk.size()
				+ " map outputs on disk. " + "Triggering merge of "
				+ copier.ioSortFactor + " files");
		// 1. Prepare the list of files to be merged. This list is prepared
		// using a list of map output files on disk. Currently we merge
		// io.sort.factor files into 1.
		for (int i = 0; i < copier.ioSortFactor; ++i) {
			Datum filestatus = copier.mapOutputFilesOnDisk.first();
			assert ! filestatus.isInMemory();
			copier.mapOutputFilesOnDisk.remove(filestatus);
			mapFiles.add(filestatus);
			approxOutputSize += filestatus.size;
		}

		
		copier.lock.unlock();
		
		// add the checksum length
		approxOutputSize += getChecksumLength(approxOutputSize, bytesPerSum);

		
		// 2. Start the on-disk merge process
//		HMergeQueue mrgQue = new HMergeQueue(mapFiles);
		// (read, write valueCoutnters);
//		Datum outmrg=mrgQue.mergeMapper(copier.ioSortFactor, 0, this, tLog,
//				copier.getTask().getTaskTracker().getHdd(), copier.getCounters());
		
		Datum outmrg=HMergeQueue.mergeToHard(copier.ioSortFactor,0, this, tLog,	
				copier.getTask().getTaskTracker() , 
				copier.getCounters(),mapFiles, null );// copier.getJobinfo().getCombiner()
		// TODO check final merge
		// Merger.writeFile(iter, writer, reporter, conf);

		
//		mapOutputFilesOnDisk.add(outmrg);

		tLog.info("DSK-Merger "+copier + " Finished merging " + mapFiles.size()
				+ " map output files on disk of total-size " + approxOutputSize
				+ "." + " Local output file is " + outmrg.name + " of size "
				+ outmrg.size);

//		copier.mapOutputFilesOnDisk.clear();
		
		isMerging.set(false);
		
		return outmrg;
	}

	private void shuffleToDisk(Datum mapOutputLoc, double mapOutputLength) {
		// Find out a suitable location for the output on local-filesystem

		Datum mapOutput = new Datum(mapOutputLoc, false);
		mapOutput.setInMemory(false);
		// rfs.read(size, user, returnTag, object)
		// fs.write()
//		logger.info("Read " + mapOutputLength + " bytes from map-output for "
//				+ mapOutputLoc.getLocation());
		tLog.info("DSK-Merger Read " + mapOutputLength + " bytes from map-output for "
				+ mapOutputLoc.getLocation());

//		sim_schedule(get_id(), 600+ rnd.nextInt(100), HTAG.shuffle_return.id(), mapOutput);
		HCopier hcopier=copier.getTask().getJobTracker().getCopier();
		hcopier.copy(mapOutput.getLocation(), copier.getTask().getLocation(),
				mapOutputLength, this, HTAG.shuffle_return.id(),
				mapOutput, Type.hard_hard);
		
	}

	static Random rnd=new Random(0);

	private AtomicBoolean isMerging = new AtomicBoolean(false);

//	public Datum lastMergeAndJoin(String msg) {
//		sim_schedule(get_id(), 0.0, HTAG.all_shuffles_finished.id());
//		Sim_predicate p = new Sim_type_p(HTAG.all_shuffles_finished_return.id());
//		Sim_event ev = new Sim_event();
//
//		sim_wait_for(p, ev);
//		return (Datum)ev.get_data();
//	}

	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
//		hlog.info("counters: "+ counters);
		hlog.save();
	}


}