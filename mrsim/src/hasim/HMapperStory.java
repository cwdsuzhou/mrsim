package hasim;

import org.apache.log4j.Logger;

import static hasim.Tools.format;
import hasim.CopyObject.Type;
import hasim.HTask.Status;
import hasim.core.CPU;
import hasim.core.Datum;
import hasim.core.HDD;
import hasim.json.JsonAlgorithm;
import hasim.json.JsonDatum;
import hasim.json.JsonJob;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.codehaus.jackson.io.MergedStream;

import dfs.MapBuffer;

import eduni.simjava.Sim_system;

public class HMapperStory extends HStory{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HMapperStory.class);

	final public static double INIT_TIME=3.0;







	public HMapperStory(String name, JobInfo jobinfo) {
		super(name, jobinfo);
		spills=new ArrayList<Datum>();
	}

	//	double mapOutputRecord	;
	//	double mapOutputSize	;
	//	double avMapRecordSize;
	double maxSpill_r_thr;
	//	Datum spill, input_spill;
	//	Datum datumCpu;
	//	double numberOfSpills, input_numberOfSpills;
	//	
	//	double totalRead,totalWrite;
	//	double shuffleSize;
	//	
	//	Datum shuffleDatum;

	JsonDatum inputSplit=null;
	Datum outputSplit=null;

	List<Datum> spills;

	public void setInputSplit(JsonDatum inputSplit){
		this.inputSplit=inputSplit;
		hlog.info("setinput split "+ inputSplit.toString());
	}
	public JsonDatum getInputSplit(){
		return inputSplit;
	}

	HMapperStory copy(HMapperStory m){
		HMapperStory result=new HMapperStory(name,m.getJobinfo());
		//		result.mapOutputRecord=m.mapOutputRecord;
		//		result.mapOutputSize=m.mapOutputSize;
		//		result.avMapRecordSize = m.avMapRecordSize;
		//		result.maxSpill_r_thr=m.maxSpill_r_thr;
		//		
		//		if(m.spill != null) 
		//			result.spill=new Datum(m.spill);
		//		if(input_spill != null)
		//			result.input_spill=new Datum(m.input_spill);
		//		
		//		result.numberOfSpills=m.numberOfSpills;
		//		result.input_numberOfSpills=m.input_numberOfSpills;
		//		
		//		result.totalRead =m.totalRead;
		//		result.totalWrite=m.totalWrite;
		//		result.shuffleSize=m.shuffleSize;
		return result;
	}

	public void generate(HTask task){
		super.generate(task);




		//set the local splits
		Set<String> machines=task.getJobTracker().getTaskTrackers().keySet();

		assert machines.size() > 0;

		inputSplit.getReplica().retainAll(machines);			
		assert inputSplit.getReplica().size() > 0;




		String spillLocation=null;
		if(inputSplit.getReplica().contains(task.getLocation())){
			hlog.info("local import read from machine "+ task.getLocation());
			spillLocation = task.getLocation();
		}else{

			int r=Tools.rnd.nextInt(inputSplit.getReplica().size());
			spillLocation = inputSplit.getReplica().get(r);
			hlog.info("remote import form "+ spillLocation+ ", to "+ task.getLocation());
		}


		// calc thresholds


		hlog.info("input split, size = "+ format(inputSplit.getSize())+
				", records =" +format(inputSplit.getRecords()));

		if(job.getNumberOfReducers()==0){
			Datum d=new Datum(inputSplit.getName(), inputSplit.getSize(),
					inputSplit.getRecords(), spillLocation);
			spills.add(d);
		}else{
			MapBuffer mb=MapBuffer.createMapBuffer(job);

			hlog.info("MapBuffer:\n"+mb);

			double outSpillRecords=mb.getSpillRecords(alg.getMapOutAvRecordSize());
			double inSpillRecords=outSpillRecords /alg.getMapRecords();

			hlog.info("outSpill records: "+ format(outSpillRecords)+
					", outspill size = "+format(outSpillRecords*
							(alg.getMapOutAvRecordSize())));

			final double	numberOfSpills= inputSplit.getRecords() /
			inSpillRecords;

			double inSpillSize = inputSplit.getSize()/numberOfSpills;



			Datum spill=new Datum("spill", inSpillSize,
					inSpillRecords,spillLocation);

			for (int i = 0; i < (int)numberOfSpills; i++) {
				Datum d= new Datum(name+"-spill-"+i,spill );
				spills.add(d);
			}
			{
				double remain=numberOfSpills-(int)numberOfSpills;
				Datum d= new Datum(name+"-spill-"+spills.size(), spill, remain);
				spills.add(d);
			}
			hlog.info("number of Spills = "+ format(numberOfSpills));
			hlog.info("one input task spill ="+spill);
		}
		hlog.info("generate(task:"+task+")");
		hlog.info("Spills.size() = "+ spills.size());


	}




	@Override
	public String toString() {
		return name;
	}
	public String toString2(){
		StringBuffer result=new StringBuffer();
		try {
			Class cls=Class.forName(this.getClass().getName());
			Field[] flds= cls.getDeclaredFields();
			for (Field fld : flds) 
				result.append( "\n"+fld.getName()+":"+fld.get(this));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}

	public static void main(String[] args) throws Exception{

		List<String> list=new ArrayList<String>();
		list.add("m1");
		list.add("m2");
		list.add("m3");

		String s1=new String("m1");

		Set<String> set=new LinkedHashSet<String>();
		set.add("m3");
		set.add(s1);
		set.add("m2");

		System.out.println("set "+ set);
		System.out.println("lins "+ list);

		list.retainAll(set);
		System.out.println("list retainAll "+ list);
	}
	public String getName() {
		return name;
	}

	@Override
	public void taskStart(HTask task) {
		hlog.info("start on task "+ task);
		setStatus(Status.running);
		task.sim_process(INIT_TIME);
	}

	@Override
	public void taskCleanUp(HTask task) {

		hlog.info("clean up");


		counters.set(CTag.STOP_TIME, Sim_system.clock());
		double duration=counters.get(CTag.STOP_TIME)-
		counters.get(CTag.START_TIME);
		counters.set(CTag.DURATION, duration);

		hlog.infoCounter("Counters:", counters);
		setStatus(Status.finished);
	}

	@Override
	public void taskProcess(HTask task) {
		hlog.info("taskProcess", Sim_system.clock());
		assert task != null;

		List<Datum> outSpills=new ArrayList<Datum>();

		//import all
		HCopier copier=task.getJobTracker().getCopier();
		for (Datum spill : spills) {
			//import spill over the HDFS 
			hlog.info("import split "+spill.size);
			CopyObject cpo=copier.copy(spill.getLocation(), task.getLocation(),
					spill.size, task, HTAG.import_split.id(), spill, Type.hard_mem);
			Datum returnSpill=(Datum)Datum.collectOne(task, HTAG.import_split.id);
			assert returnSpill==spill;
			hlog.info("spill imported cpo.id="+ cpo.id);
		}
		for (Datum spill : spills) {

			hlog.info("map split "+ spill.name);
			Datum result=map(spill);			
			assert ! result.isInMemory();
			outSpills.add(result);

		}





		int partitions = job.getNumberOfReducers();


		if(partitions >0){

			hlog.info("start merging ");

			if(outSpills.size()>1){
			outputSplit=HMergeQueue.mergeToHard(job.getIoSortFactor(),0, task,
					hlog, task.getTaskTracker(),counters, outSpills, jobinfo.getCombiner());
			}else{
				outputSplit=new Datum(outSpills.get(0));
			}
			outputSplit.setLocation(task.getLocation());

			hlog.info("generate output split:"+ outputSplit);

			hlog.info("merg done");

			double fraction= 1.0/(double)partitions;


			hlog.info("add map results to addScheduled copies, " +
					"fraction = "+format(fraction));

			assert getJobinfo().reducersFinished.size()==0;
			List<HStory> allReducers=new ArrayList<HStory>(getJobinfo().reducersWaiting);
			allReducers.addAll(getJobinfo().reducersRunning);

			int numRed=job.getNumberOfReducers();

			List<Double> fractions=Tools.getAllFractions(numRed, 
					1.0/numRed, 2.0/numRed);
			assert numRed == fractions.size();


			for (int i = 0; i < fractions.size(); i++) {
				HStory story=allReducers.get(i);
				double f=fractions.get(i);
				Datum dtm=new Datum(outputSplit, f);
				dtm.setInMemory(false);
				story.addScheduledCopy(dtm);
			}
			//			for (HStory rStory : getJobinfo().reducersWaiting) {
			//				Datum d=new Datum(outputSplit,fraction);
			//				d.setInMemory(false);
			//				rStory.addScheduledCopy(d);
			//			}
			//			for (HStory story : getJobinfo().reducersRunning) {
			//				Datum d=new Datum(outputSplit,fraction);
			//				d.setInMemory(false);
			//				story.addScheduledCopy(d);
			//			}
		}else{

			hlog.info("write to hdfs");
			//			//no reducer write to hdfs
			//			for (Datum spill : outSpills) {
			//				copier.hdfsReplicate(job.getReplication(), task.location, 
			//						spill.size, task, HTAG.replicate.id(), spill);
			//				
			//				Datum returnSpill=(Datum)Datum.collectOne(task, HTAG.replicate.id());
			//				if(returnSpill.id() != spill.id()){
			//					logger.error("spill id = "+ spill.id());
			//					logger.error("return spill id = "+ returnSpill.id());
			//				}
			//				assert returnSpill==spill;
			//				counters.inc(CTag.HDFS_BYTES_WRITTEN, spill.size);
			//				
			//				hlog.info("write spill "+ spill);
			//			}
		}


	}



}