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
		CPU cpu=task.getTaskTracker().getCpu();
		HDD hdd=task.getTaskTracker().getHdd();
		HCopier copier=task.getJobTracker().getCopier();
		
		double oneRecordSize=alg.getMapOutAvRecordSize();
		if(job.getNumberOfReducers()>0){
			oneRecordSize=(alg.getMapOutAvRecordSize()+ REF_2);
		}
		
		
		double mapOutPutBytes= alg.getMapOutAvRecordSize()* spill.records;

		double cpuCost=spill.records* alg.getMapCost();
		double outRecords=spill.records*alg.getMapRecords();
		double outSize=outRecords* oneRecordSize;
//		double outSize=5852245050.0;
		
		//import spill over the HDFS 
//		copier.copy(spill.getLocation(), task.getLocation(),
//				spill.size, task, HTAG.import_split.id(), spill, Type.hard_mem);

		
		//process the map
		cpu.work(cpuCost, task.get_id(), HTAG.cpu_split.id(), spill);

		Datum cpuReturn=(Datum)Datum.collectOne(task, HTAG.cpu_split.id);
		assert cpuReturn==spill;
		
		int numReducers=job.getNumberOfReducers();
		if(numReducers==0){
			int replication=job.getReplication();
			copier.hdfsReplicate(replication,task.location, 
					outSize, task, HTAG.hdd_split.id(), spill);
			counters.inc(CTag.HDFS_BYTES_WRITTEN, outSize);
			Datum.collectOne(task ,	HTAG.hdd_split.id);

		}else{
		//write outspill to file
			hdd.write(outSize, task, HTAG.hdd_split.id(), spill);
			Datum.collectOne(task ,	HTAG.hdd_split.id);
			counters.inc(CTag.MAP_INPUT_RECORDS, spill.records);
		
			counters.inc(CTag.MAP_OUTPUT_BYTES, mapOutPutBytes);
			counters.inc(CTag.MAP_OUTPUT_RECORDS, outRecords);
			
			counters.inc(CTag.FILE_BYTES_WRITTEN, outSize);
			counters.inc(CTag.SPILLED_RECORDS, outRecords);
		}
		
		
		counters.inc(CTag.HDFS_BYTES_READ, spill.size);
		counters.inc(CTag.MAP_INPUT_SIZE, spill.size);
		
		
		Datum result=new Datum(spill.getName()+"-m",outSize,outRecords);
		result.setInMemory(false);

		return result;
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
