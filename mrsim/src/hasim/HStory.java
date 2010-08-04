package hasim;

import dfs.MapBuffer;
import eduni.simjava.Sim_system;
import hasim.CopyObject.Type;
import hasim.HTask.Status;
import hasim.core.CPU;
import hasim.core.Datum;
import hasim.core.HDD;
import hasim.json.JsonAlgorithm;
import hasim.json.JsonJob;

public abstract class HStory implements HLoggerInterface{

	final public static double REF_2=2.0;

	final protected  String name;
	final protected JsonJob job;
	final protected JsonAlgorithm alg;
	final protected HCounter counters = new HCounter();
	protected Status status=Status.idle;
	final protected HLogger hlog;
	final protected JobInfo jobinfo;
	
	
	

	protected HTask task;
	
	public HTask getTask() {
		return task;
	}

	public void setTask(HTask task) {
		this.task = task;
	}

	public HStory(String name, JobInfo jobinfo)  {
			this.jobinfo=jobinfo;
			this.name=name;
			this.job=jobinfo.getJob();
			this.alg=jobinfo.getAlgorithm();
			hlog=new HLogger(name );	
	}
	
	public void generate(HTask task){
		setTask(task);
		counters.set(CTag.START_TIME, Sim_system.clock());
		hlog.info("Assign story to task "+ task.get_name() + 
				" location ="+task.getLocation());
	}
	
	abstract public void taskStart(HTask task);

	abstract public void taskProcess(HTask task);
	
	abstract public void taskCleanUp(HTask task);
	
	
	synchronized public void addScheduledCopy(Datum datum){
		hlog.info("add map to copy , datum="+ datum);
	}

	
	public JobInfo getJobinfo() {
		return jobinfo;
	}

	@Override
	public HLogger getHlog() {
		return hlog;
	}

	public HCounter getCounters() {
		return counters;
	}

	public Status getStatus() {
		return status;
	}
	public void setStatus(Status status) {
		this.status = status;
		hlog.info("setStatus("+status.name()+")");
	}
	
	
	@Override
	public String toString() {
		return name;
	}

	public HTask getHTask() {
		// TODO Auto-generated method stub
		return task;
	}

	public Datum map(Datum spill){
		
		counters.inc(CTag.HDFS_BYTES_READ, spill.size);
		counters.inc(CTag.MAP_INPUT_RECORDS, spill.records);
		counters.inc(CTag.MAP_INPUT_SIZE, spill.size);

		CPU cpu=task.getTaskTracker().getCpu();
		HDD hdd=task.getTaskTracker().getHdd();
		HCopier copier=task.getJobTracker().getCopier();
		
		double mapOutAvRecordSize=alg.getMapOutAvRecordSize();
		if(job.getNumberOfReducers()>0){
			mapOutAvRecordSize=(alg.getMapOutAvRecordSize()+ REF_2);
		}
		
		
		// double mapOutPutBytes= alg.getMapOutAvRecordSize()* spill.records;

		double cpuCost = spill.records * alg.getMapCost();
		double mapOutRecords = spill.records * alg.getMapRecords();
		double mapOutSize = mapOutRecords * mapOutAvRecordSize;

		//process the map
		cpu.work(cpuCost, task.get_id(), HTAG.cpu_split.id(), spill);
		Datum cpuReturn=(Datum)Datum.collectOne(task, HTAG.cpu_split.id);
		assert cpuReturn==spill;
		
		Datum outSpill =new Datum(spill.name+"-out",mapOutSize, mapOutRecords);
		Datum outSpillCombined=outSpill;

		int numReducers=job.getNumberOfReducers();
		if(numReducers==0){
			//write outspill directly to hdfs
			int replication=job.getReplication();
			copier.hdfsReplicate(replication,task.location, 
					mapOutSize, task, HTAG.hdd_split.id(), spill);
			counters.inc(CTag.HDFS_BYTES_WRITTEN, mapOutSize);
			Datum.collectOne(task ,	HTAG.hdd_split.id);

		}else{
		//sort, combine, and write outspill to file
			
			//sort and combine before spilling
			HCombiner combiner = jobinfo.getCombiner();
			if(combiner != null){
				hlog.info("spill combine pass combine input "+outSpill.records );

				counters.inc(CTag.COMBINE_INPUT_RECORDS, outSpill.records);
				outSpillCombined = combiner.combine(outSpill);
				//cpu combine cost
				counters.inc(CTag.COMBINE_OUTPUT_RECORDS, outSpillCombined.records);
				hlog.info("spill combine pass combine out "+outSpillCombined.records );

			}
			
			hdd.write(outSpillCombined.size, task, HTAG.hdd_split.id(), outSpillCombined);
			Datum.collectOne(task ,	HTAG.hdd_split.id);
			
		
			counters.inc(CTag.MAP_OUTPUT_BYTES, outSpill.size);
			counters.inc(CTag.MAP_OUTPUT_RECORDS, outSpill.records);
			
			counters.inc(CTag.FILE_BYTES_WRITTEN, outSpillCombined.size);
			counters.inc(CTag.SPILLED_RECORDS, outSpillCombined.records);
		}
		
		
		
//		Datum result=new Datum(spill.getName()+"-m",mapOutSize,mapOutRecords);
		outSpillCombined.setInMemory(false);

		return outSpillCombined;
	}
	
	public Datum reduce(Datum dToReduce){
		
		CPU cpu=task.getTaskTracker().getCpu();
		HDD hdd=task.getTaskTracker().getHdd();
		HCopier copier=task.getJobTracker().getCopier();

		double cpuCost=dToReduce.records* alg.getReduceCost();
		double outRecords=dToReduce.records*alg.getReduceRecords();
		double outSize=outRecords* alg.getReduceOutAvRecordSize();
		
		

		cpu.work(cpuCost, task.get_id(), HTAG.reducer_CPU_reduce.id(), dToReduce);
		
		copier.hdfsReplicate(job.getReplication(), task.location,
				outSize, task, HTAG.reduce_HDFS_reduce.id(), dToReduce);
//		hdd.write(outSize, task.get_id(), HTAG.reduce_HDFS_reduce.id(), dToReduce);
		
		
		Datum.collect(task,	HTAG.reducer_CPU_reduce.id(), HTAG.reduce_HDFS_reduce.id());
		
		counters.inc(CTag.REDUCE_INPUT_RECORDS, dToReduce.records);
		counters.inc(CTag.REDUCE_SHUFFLE_BYTES, dToReduce.size);
		
		counters.inc(CTag.REDUCE_OUTPUT_RECORDS, outRecords);

		counters.inc(CTag.HDFS_BYTES_WRITTEN, outSize);
		
		Datum result=new Datum(dToReduce.getName()+"-r",outSize,outRecords);
		return result;
	}

}
