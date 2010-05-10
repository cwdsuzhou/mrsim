package hasim;

import org.apache.log4j.Logger;
import org.apache.log4j.lf5.viewer.configure.MRUFileManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import dfs.Pair;
import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_system;
import eduni.simjava.Sim_type_p;

import gridsim.datagrid.storage.HarddriveStorage;
import hasim.CopyObject.Step;
import hasim.CopyObject.Type;
import hasim.HTask.Status;
import hasim.core.CPU;
import hasim.core.Datum;
import hasim.core.HDD;
import hasim.json.JsonAlgorithm;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;

public class HReducerStory extends HStory{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HReducerStory.class);




//	List<Pair<String, Datum>> mapCopiedInMem=new ArrayList<Pair<String,Datum>>();
//	List<Pair<String, Datum>> mapCopied=new ArrayList<Pair<String,Datum>>();


	//List<Pair<String, Double>> mapOutput;
	List<Datum> scheduledCopies=new ArrayList<Datum>();
	
	int globalInMem=0;

//	double avReduceRecordSize;
//	double reduceOutputRecord	;
//	double reduceOutputSize	;
//
//	double shuffleSize;
//
//	double inputRecords;
//	double records=0;
//	double size=0;
//	double cpuCost;


//	double jobShuffleMerge;
//	int immemMergeThreshold;
//	double oneMapLimit;
	//	double maxSpill_r_thr;
	//	Datum spill, input_spill;
	//	double numberOfSpills, input_numberOfSpills;
	//	
	double totalRead,totalWrite;




	public HReducerStory(String name, JobInfo jobinfo) {
		super(name, jobinfo);
	}

	Lock mapLock=new ReentrantLock(true);
	
	synchronized public void addScheduledCopy(Datum datum){
		hlog.info("add map to copy , datum="+ datum);
		mapLock.lock();
		scheduledCopies.add(datum);
		mapLock.unlock();
	}
//	public void generate() {
//		if(jobinfo==null){
//			logger.error("jobinfo==null");
//			return;
//		}
//		
//		int numOfReducers=job.getNumberOfReducers();
//		inputRecords=job.getData().getRecords()* alg.getMapRecords()/numOfReducers;
//		records=inputRecords * alg.getReduceRecords();
//
//		size= job.getData().getSize()*alg.getMapSize()*
//			alg.getReduceSize()/numOfReducers;
//		cpuCost=inputRecords * alg.getReduceCost();
//		
//		double memoryHeap=job.getMapredChildJavaOpts()*1024*1024.0;
//		double jobShuffleInputBuffer=memoryHeap * job.getMapredJobShuffleInputBufferPercent();
//		double jobShuffleMerge= jobShuffleInputBuffer * job.getMapredJobShuffleMergePercent();
//		int immemMergeThreshold=job.getMapredInmemMergeThreshold();
//		double oneMapLimit = 0.25 * jobShuffleMerge;
//		
//		
//	}
	public HLogger getHlog() {
		return hlog;
	}
	public JobInfo getJobinfo() {
		return jobinfo;
	}

	@Override
	public void taskCleanUp(HTask task) {
		hlog.info("clean up");

		counters.set(CTag.STOP_TIME, Sim_system.clock());
		
		double duration=counters.get(CTag.STOP_TIME)-
			counters.get(CTag.START_TIME);
		
		counters.set(CTag.DURATION, duration);
		
		hlog.infoCounter("Counters:",counters);
		setStatus(Status.finished);
		
	}

	@Override
	public void taskProcess(HTask task) {
		
		hlog.info("task process");
		
		hlog.info("global inmem:"+ globalInMem);

		List<Datum> hardDatums= taskCopyAndMerge(task);
		HMergeQueue mergeQue=new HMergeQueue(hardDatums);
		
		globalInMem=0;
		Datum dToReduce=mergeQue.mergeReducer(job.getIoSortFactor(), globalInMem, task);
		
		hlog.info("datum to reduce:"+ dToReduce);
		
		CPU cpu=task.getTaskTracker().getCpu();
		HDD hdd=task.getTaskTracker().getHdd();
		

		double outRecords=dToReduce.records*alg.getReduceRecords();
		double outSize=outRecords* alg.getReduceOutAvRecordSize();
		
		cpu.work(dToReduce.records* alg.getReduceCost(), task.get_id(), 
				HTAG.reducer_CPU_reduce.id(), dToReduce);
		hdd.write(outSize, task, HTAG.reduce_HDFS_reduce.id(), dToReduce);
		
		Datum.collect(task, HTAG.reducer_CPU_reduce.id(), HTAG.reduce_HDFS_reduce.id());
		
		counters.inc(CTag.HDFS_BYTES_WRITTEN, outSize);
		counters.inc(CTag.REDUCE_OUTPUT_RECORDS, outRecords);
		
	}

	@Override
	public void taskStart(HTask task) {
		hlog.info("start on task "+ task);
		setStatus(Status.running);
		counters.set(CTag.START_TIME, Sim_system.clock());
	}

	@Override
	public void generate(HTask task) {
		super.generate(task);		
	}

	
	/**
	 * 
	 * @param task
	 * @return list of in-memory datums and on hard datums (in-memory ones are at first)
	 */
	private List<Datum> taskCopyAndMerge(HTask task){
		
		hlog.info("taskCopyAndMerge");
		HDD hdd=task.getTaskTracker().getHdd();
		
		final int sortFactor = job.getIoSortFactor();
		final double javaHeap=job.getMapredChildJavaOpts()*1024*1024.0;
		final double jobShuffleInputBuffer=javaHeap* job.getMapredJobShuffleInputBufferPercent();
		//ammount of memory available for inmem data
		double memoryLimit=jobShuffleInputBuffer* job.getMapredJobShuffleMergePercent();
		if(job.getMemoryLimit() > 0){
			//TODO check later
			memoryLimit=job.getMemoryLimit();
		}
		final double memoryThreshould=memoryLimit* job.getMapredJobShuffleInputBufferPercent()
			*job.getMapredJobShuffleMergePercent()		;//0.66
		hlog.info("threshould "+ memoryLimit+"* "+ job.getMapredJobShuffleInputBufferPercent()
				+" * "+ job.getMapredJobShuffleMergePercent()+"= "+ memoryThreshould);
		final int immemMergeThreshold=job.getMapredInmemMergeThreshold();
		final int orgParalleleCopies=job.getMapReduceParallelCopies();
		
		final double maxSingleShuffleLimit=memoryLimit* 0.25;
		final double heartbeat=task.getJobTracker().getRack().getHeartbeat();
		
		double memBuffer=0; // up to jobShuffleMerge;
		int availableMaps=immemMergeThreshold;
		int parallelCopies=orgParalleleCopies;
		
		 
		 hlog.info("MemoryLimit="+ memoryLimit+", MaxSingleShuffleLimit="+maxSingleShuffleLimit);
		
		hlog.info("memBuffer: "+memBuffer+
				"\navailableMaps: "+ availableMaps+
		"\nmemthreshould: "+ memoryThreshould);
		
		HCopier copier=task.getJobTracker().getCopier();
		
		hlog.info("task copying on "+ task);
		setStatus(Status.copying);
		
		
		assert parallelCopies > 0;
		
		//move local map output to mapCopiedMem
//		List<Datum> localDatums=new ArrayList<Datum>();
		//List<Datum> remoteDatums=new ArrayList<Datum>();

		List<Datum> hardDatums=new ArrayList<Datum>();
		List<Datum> inMemDatums=new ArrayList<Datum>();
		//List<Datum> merges=new ArrayList<Datum>();

//		HMergeQueue mergeMemory=new HMergeQueue();
		HMergeQueue mergeDisk=new HMergeQueue();
		
		//hlog.info("remoteDatum size:"+ remoteDatums.size());


		hlog.info("start, parallels copies  "+ parallelCopies);

		int mapsCopied=0;

		while (Sim_system.running()) {

			if (mapsCopied >= job.getNumberOfMappers() ) {
				hlog.info("copied all mappers , break");
				break;
			}
			
			
			task.sim_schedule(task.get_id(), heartbeat, HTAG.reducer_check_mappers_out.id());

			Sim_event ev = new Sim_event();
			Sim_predicate p=new Sim_type_p(HTAG.reducer_check_mappers_out.id(), 
					HTAG.reducer_copy_one_map_return.id());
			task.sim_get_next(p,ev);
			
			int tag = ev.get_tag();

			if (tag == HTAG.reducer_check_mappers_out.id()) {

				assert status == Status.copying;
				
				while(parallelCopies > 0 && scheduledCopies.size() >0) {
					mapLock.lock();
					Datum cp = scheduledCopies.remove(0);
					mapLock.unlock();
					
					Type type= cp.size > maxSingleShuffleLimit? Type.hard_hard:Type.hard_mem;
					cp.setData(type);

					String stype= type == Type.hard_hard? "DISK":"RAM";
					hlog.info("Shuffling "+cp.size+" bytes ( raw bytes) into "+stype+" from "+cp.getLocation());
					
					if(cp.getLocation().equals(task.getLocation())){
						task.sim_schedule(task.get_id(), 0.1, 
								HTAG.reducer_copy_one_map_return.id(), cp);
//						if(cp.size> maxSingleShuffleLimit){
//							
//						}else{
//							//load to memory
//							hdd.read(cp.size, task.get_id(), HTAG.reducer_copy_one_map_return.id(),	 cp);
//							counters.inc(CounterTag.FILE_BYTES_READ, cp.size);
//						}
						//remoteDatums.add(cp);
						hlog.info("redcuer_copy_one_map (local) "+ cp);

					}else{
					
						copier.copy(cp.getLocation(), task.getLocation(),cp.size,
								task,HTAG.reducer_copy_one_map_return.id() ,
								cp, type);

						
						hlog.info("redcuer_copy_one_map "+ cp);
					
					}
					parallelCopies--;
					if (parallelCopies == 0 || scheduledCopies.size() == 0)
						break;//local for

				};
				continue;
			}

			if (tag == HTAG.reducer_copy_one_map_return.id()) {
				mapsCopied++;
				
				
				Datum cp=(Datum)ev.get_data();
				hlog.info("reducer_copy_one_map_return "+ cp );
				//logger.debug(getName()+",recieved data "+ dtm.id());
				
				


				if(cp.size> maxSingleShuffleLimit){
					cp.setData(Type.hard_hard);
					mergeDisk.addSegment(cp);
					
//					if(! cp.getLocation().equals(task.getLocation())){
//						counters.inc(CounterTag.FILE_BYTES_WRITTEN, cp.size);
//						counters.inc(CounterTag.SPILLED_RECORDS, cp.records);						
//					}
					
				}else{
					cp.setData(Type.hard_mem);
//					mergeMemory.addSegment(cp);
					inMemDatums.add(cp);
					memBuffer += cp.size;
					
					
				
				}
				
				if(mapsCopied== job.getNumberOfMappers()){
					hlog.info("break "+ mapsCopied);
					break;
				}
				//check if merge is needed
				if( inMemDatums.size() >= immemMergeThreshold ||
						memBuffer > memoryThreshould ){
//						||	inMemDatums.size()>=9){
					
					Datum outDatum= mergeInMem(task, inMemDatums);
					inMemDatums.clear();
					outDatum.setData(Type.hard_hard);
					hlog.info("in memory merge complete");
					hardDatums.add(outDatum);
					hlog.info("harddatums.size:"+ hardDatums.size());
					assert memBuffer-outDatum.size<1e-6;
					memBuffer=0;
				}
			
				if(mergeDisk.getSegments().size()>= sortFactor){
					Datum outDatum=mergeDisk.mergeMapper(sortFactor, 0, task, hlog,
							task.getTaskTracker().getHdd(), counters);
					hlog.info("in hard disk merge complete");
					outDatum.setData(Type.hard_hard);
					hardDatums.add(outDatum);
				}
				
				parallelCopies++;

				hlog.info("parallelCopies "+ parallelCopies);

				continue;

			}
		}
		
		globalInMem=inMemDatums.size();
		
		Datum memdatum=mergeInMem(task, inMemDatums);
		memdatum.setData(Type.hard_hard);
		hardDatums.add(memdatum);
//		for (Datum datum : inMemDatums) {
//			assert datum.getData() instanceof Type;
//			Type t=(Type)datum.getData();
//			assert t== Type.hard_mem;
//			hardDatums.add(0, datum);
//		}
		// all mappers copied
		hlog.info("finish copying");
		String rs=new String("hardDatums :");
		for (Datum datum : hardDatums) {

			rs+= "\n"+datum+ ", "+ datum.getData();
		}
		hlog.info(rs );
		return hardDatums;

	}
	
	public Datum mergeInMemAndHard(HTask task, List<Datum> list){
		assert list != null && task != null;
		HDD hdd= task.getTaskTracker().getHdd();
		List<Datum> inmems=new ArrayList<Datum>();
		for (Iterator<Datum> iter = inmems.iterator(); iter.hasNext();) {
			Datum d=iter.next();
			assert d.getData() instanceof Type;
			Type type=(Type)d.getData();
			
			if(type == Type.hard_mem){
				inmems.add(d);
				iter.remove();
			}
		}
		hlog.info("going to merge");
		Datum inMemDatum= mergeInMem(task, inmems);
		return inMemDatum;
		
	}
	public Datum mergeInMem(HTask task,List<Datum> list){
		
		hlog.info("merge "+ list.size()+", segments in memory");
		assert list != null && task != null;
		HDD hdd= task.getTaskTracker().getHdd();
		
		double totalSize=0;
		double totalRecords=0;
		
		for (Datum d : list) {
			assert d.getData() instanceof Type;
			assert (Type)d.getData()==Type.hard_mem;
			
			totalSize += d.size;
			totalRecords += d.records;
		}
		
		Datum result=new Datum("mrg-"+ name, totalSize, totalRecords,task.getLocation());
		hdd.write(totalSize, task, HTAG.rStory_mergeInMem.id(), result);
		
		Datum resultBack= (Datum)Datum.collectOne(task, HTAG.rStory_mergeInMem.id());
		
		counters.inc(CTag.FILE_BYTES_WRITTEN, totalSize);
		
		counters.inc(CTag.SPILLED_RECORDS, totalRecords);
		//TODO check if counter of spilled records is needed
		assert result == resultBack;
		result.setData(Type.hard_hard);
		return result;
	}
//	public void taskStart(HReducerTask task) {
//		hlog.info("start on task "+ task);
//		setStatus(Status.running);
//		counters.set(CounterTag.START_TIME, Sim_system.clock());
//		
//	}
//	public void taskProcess(HReducerTask task){
//		hlog.info("process on task "+ task);
//		HMergeQueue mrgQue=taskCopying(task);
//		
//		
//		task.setStatus(Status.sort);
//		taskSort(task);
//		
//		
//		mrgQue.mergeReducer(job.getIoSortFactor(), 0, task);
//		
//	}

//	private void taskSort(HReducerTask task) {
//
//		hlog.info("task sort");
//		setStatus(Status.sort);
//	}
//	public void taskCleanUp(HReducerTask task){
//		counters.set(CounterTag.STOP_TIME, Sim_system.clock());
//		hlog.info("Counters:"+counters.toString("\n\t"));
//		
//		setStatus(Status.finished);
//	}
	
	
//	private HMergeQueue taskCopying(HReducerTask task){
//		final double javaHeap=job.getMapredChildJavaOpts()*1024*1024.0;
//		final double jobShuffleInputBuffer=javaHeap* job.getMapredJobShuffleInputBufferPercent();
//		final double jobShuffleMerge=jobShuffleInputBuffer* job.getMapredJobShuffleMergePercent();
//		final int immemMergeThreshold=job.getMapredInmemMergeThreshold();
//		final int orgParalleleCopies=job.getMapReduceParallelCopies();
//
//		final double MaxMapSize=jobShuffleMerge* 0.25;
//		double memBuffer=jobShuffleMerge;
//		int availableMaps=immemMergeThreshold;
//		int parallelCopies=orgParalleleCopies;
//		
//		hlog.info("memBuffer: "+memBuffer+
//				"\navailableMaps: "+ availableMaps+
//				"\noneMapLimit: "+ MaxMapSize);
//		
//		HMergeQueue mergQueue=new HMergeQueue();
//		HDD hdd=task.getTracker().getHdd();
//		
//		hlog.info("task copying on "+ task);
//		setStatus(Status.copying);
//		
//		//int sortFactor = job.getIoSortFactor();
//		assert parallelCopies > 0;
//
//		hlog.info("start, parallels copies  "+ parallelCopies, Sim_system.clock());
//		task.sim_schedule(task.get_id(), 0.0, HTAG.reducer_check_mappers_out.id());
//		
//		//move local map output to mapCopiedMem
//		for (Iterator<Pair<String, Double>> iterator = mergQueue.iterator(); iterator.hasNext();) {
//			Datum datum = (Datum) iterator.next();
//			
//		}
//		
//		while (Sim_system.running()) {
//			Sim_event ev = new Sim_event();
//			task.sim_get_next(ev);
//			int tag = ev.get_tag();
//		
//			if (tag == HTAG.reducer_check_mappers_out.id()) {
//
//				assert status == Status.copying;
//
//				task.sim_schedule(task.get_id(), task.heartBeat, HTAG.reducer_check_mappers_out.id());
//
//		
//				
//				while(parallelCopies > 0 && mapToCopy.size() >0) {
//	
//					Pair<String, Datum> cp = mapToCopy.remove(0);
//					// send to receiver reducer_copy_one_map_return
//					Datum dtosend=c;
//					dtosend.data=cp;
//					dtosend.register(task, HTAG.reducer_copy_one_map_return.id());
//					
//					Type type;
//					if(cp.getK().equals(task.getLocation())){
//						//local copy
//					}
//					CopyObject cpo=new CopyObject(cp.getV(), Type.hard_hard,cp.getK(), 
//							task.getLocation(), task, HTAG.reducer_copy_one_map_return.id(), cp);
//					HCopier copier=task.getJobTracker().getCopier();
//					copier.copy(cpo);
//					
//					hlog.info("redcuer_copy_one_map "+ cpo);
//					
////					sim_schedule(get_id(), 2.5 + i * 0.1,
////							HTAG.reducer_copy_one_map_return.id(), cp);
//
//					parallelCopies--;
//					if (parallelCopies == 0 || mapToCopy.size() == 0)
//						break;//local for
//
//				};
//				continue;
//			}
//
//			if (tag == HTAG.reducer_copy_one_map_return.id()) {
//				
//				CopyObject cpo=(CopyObject)ev.get_data();
////				Datum dtm=(Datum)ev.get_data();
//				Pair<String, Double> cp=(Pair<String, Double>)cpo.getO();
//				
//				hlog.info("reducer_copy_one_map_return "+ cpo + " cp="+cp );
//				//logger.debug(getName()+",recieved data "+ dtm.id());
//				
//			
//				mergQueue.addSegment("cpo",cp.getV(),cp.getV());
//				
//				parallelCopies++;
//				mapCopied.add(cp);
//
//				hlog.info("parallelCopies "+ parallelCopies);
//				hlog.info("mapCopied "+ mapCopied.size());
//
//				if (mapCopied.size() == job.getNumberOfMappers()) {
//					// all mappers copied
//					hlog.info("finish copying");
//					//return to main body
//					break;
//				}
//
//				task.sim_schedule(task.get_id(), task.heartBeat, HTAG.reducer_check_mappers_out
//						.id());
//
//				continue;
//
//			}
//		}
//		return mergQueue;
//	}
}
