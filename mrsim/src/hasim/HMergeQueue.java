package hasim;

import org.apache.log4j.Logger;

import addition.HCounterValue;

import eduni.simjava.Sim_entity;

import hasim.CopyObject.Type;
import hasim.core.CPU;
import hasim.core.CpuTest;
import hasim.core.Datum;
import hasim.core.HDD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.management.ImmutableDescriptor;




public class HMergeQueue
extends HPriorityQueue implements HIterator {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HMergeQueue.class);

	private static final float CHKSUM_AS_FRACTION = 0.0f;
//		private static final float CHKSUM_AS_FRACTION = 0.01f;

	//    CompressionCodec codec;

	private List<Datum> segments ;
	private HCombiner combiner;

	public HCombiner getCombiner() {
		return combiner;
	}


	public void setCombiner(HCombiner combiner) {
		this.combiner = combiner;
	}


	public List<Datum> getSegments() {
		return segments;
	}

	private long totalBytesProcessed;
	private float progPerByte;
	private HProgress mergeProgress = new HProgress();

	//Progressable reporter;


	Datum minSegment;
	Comparator<Datum> segmentComparator = new Comparator<Datum>() {

		//TODO added by suhel to check if datum in memory or in hard
		@Override
		public int compare(Datum o1, Datum o2) {
			if( o1.getData() instanceof Type && o2.getData() instanceof Type){

				if(o1.isInMemory() == o2.isInMemory()){
					return o1.compareTo(o2);
				}else{
					if(o1.isInMemory())
						return -1;
					else
						return 1;
				}
			}else{
				return -1;
			}
		}
	};


	public HMergeQueue() {
		segments=new ArrayList<Datum>();
	}


	public HMergeQueue(Collection<Datum> segments    ) {
		this();

		for (Datum file : segments) {
			addSegment(file);
		}

		// Sort segments on file-lengths
		Collections.sort(this.segments, segmentComparator); 
	}

	public void addSegment(Datum datum){
		Datum d=new Datum(datum.getName(), datum);
		d.setInMemory(datum.isInMemory());

		d.setData(datum.getData());
		this.segments.add(d);
		Collections.sort(this.segments, segmentComparator); 
	}





	private void adjustPriorityQueue(int i) throws IOException{
		Datum reader=segments.get(i);
		boolean hasNext = i<segments.size();
		totalBytesProcessed += reader.size;
		mergeProgress.set(totalBytesProcessed * progPerByte);
		//      if (hasNext) {
		//        adjustTop();
		//      } else {
		//        pop();
		//        reader.close();
		//      }
	}

	public boolean next() throws IOException {
		if (size() == 0)
			return false;

		if (minSegment != null) {
			//minSegment is non-null for all invocations of next except the first
			//one. For the first invocation, the priority queue is ready for use
			//but for the subsequent invocations, first adjust the queue 
			//adjustPriorityQueue(minSegment);
			if (size() == 0) {
				minSegment = null;
				return false;
			}
		}
		minSegment = top();


		return true;
	}

	public Datum mergeReducer(int factor,int inMem,HTask task){

		//    	HReducerStory story=(HReducerStory)task.getStory();
		HStory story=(HStory)task.getStory();

		HLogger mlog=story.getHlog();
		HDD hdd=task.getTaskTracker().getHdd();
		CPU cpu=task.getTaskTracker().getCpu();
		assert story != null;

		//    
		//    	double readCounter=0, writeCounter=0;
		//logger.info("Merging " + segments.size() + " sorted segments");
		mlog.info("Merging " + segments.size() + " sorted segments. In memory segments"+inMem );

		int inmemTest=0;
		for (Datum d : segments) {
			Type type=(Type)d.getData();
			if(type== Type.hard_mem)
				inmemTest++;
		}
		assert inmemTest == inMem;

		//create the MergeStreams from the sorted map created in the constructor
		//and dump the final output to a file
		int numSegments = segments.size();
		int origFactor = factor;
		int passNo = 1;
		do {
			//get the factor for this pass of merge. We assume in-memory segments
			//are the first entries in the segment list and that the pass factor
			//doesn't apply to them
			factor = getPassFactor(factor, passNo, numSegments - inMem);
			if (1 == passNo) {
				factor += inMem;
				//TODO check here later
				inMem=0;
			}
			List<Datum> segmentsToMerge =new ArrayList<Datum>();
			int segmentsConsidered = 0;
			int numSegmentsToConsider = factor;
			long startBytes = 0; // starting bytes of segments of this merge
			while (true) {
				//extract the smallest 'factor' number of segments  
				//Call cleanup on the empty segments (no key/value data)
				List<Datum> mStream = 
					getSegmentDescriptors(numSegmentsToConsider, segments);

				for ( int i=0; i< mStream.size(); i++) {
					Datum segment = mStream.get(i);
					// Initialize the segment at the last possible moment;
					// this helps in ensuring we don't use buffers until we need them
					// segment.init(readsCounter);
					boolean hasNext = i < mStream.size();
					startBytes += segment.size;

					if (hasNext) {
						segmentsToMerge.add(segment);
						segmentsConsidered++;
					}
					else {
						numSegments--; //we ignore this segment for the merge
					}
				}
				//if we have the desired number of segments
				//or looked at all available segments, we break
				if (segmentsConsidered == factor || 
						segments.size() == 0) {
					break;
				}

				numSegmentsToConsider = factor - segmentsConsidered;
			}

			//feed the streams to the priority queue
			clear();
			for (Datum segment : segmentsToMerge) {
				put(segment);
				//            readCounter += segment.size;

			}

			//if we have lesser number of segments remaining, then just return the
			//iterator, else do another single level merge
			if (numSegments <= factor) {
				// Reset totalBytesProcessed to track the progress of the final merge.
				// This is considered the progress of the reducePhase, the 3rd phase
				// of reduce task. Currently totalBytesProcessed is not used in sort
				// phase of reduce task(i.e. when intermediate merges happen).
				totalBytesProcessed = startBytes;
				//calculate the length of the remaining segments. Required for 
				//calculating the merge progress
				long totalBytes = 0;

				for (int i = 0; i < segmentsToMerge.size(); i++) {
					totalBytes += segmentsToMerge.get(i).size;
				}


				if (totalBytes != 0) //being paranoid
				progPerByte = 1.0f / (float)totalBytes;

				if (totalBytes != 0)
					mergeProgress.set(totalBytesProcessed * progPerByte);
				else
					mergeProgress.set(1.0f); // Last pass and no segments left - we're done

				mlog.info("Down to the last merge-pass, with " + numSegments + 
						" segments left of total size: " + totalBytes + " bytes");

				double approxOutputSize = 0; 
				double approxOutputRecord = 0; 

				double approxInMemSize = 0;
				double approxInMemRecord = 0;

				for (Datum s : segmentsToMerge) {
					approxOutputSize += s.size * (1+ CHKSUM_AS_FRACTION );
					approxOutputRecord+= s.records;

					Type type=(Type)s.getData();
					if(type == Type.hard_mem){
						approxInMemRecord+=s.records;
						approxInMemSize+=s.size;
					}
				}

				Datum mrgSegmet = new Datum("m_" + segments.size(),
						approxOutputSize, approxOutputRecord);
				mrgSegmet.setData(Type.hard_hard);

				{

					hdd.read(mrgSegmet.size-approxInMemSize,task, HTAG.merge_read.id(), mrgSegmet);
					//no write this will be to the reduce phase
					//	            hdd.write(mrgSegmet.size,task.get_id(), HTAG.merg_write.id(),mrgSegmet);


					Datum.collect(task, HTAG.merge_read.id());


					story.getCounters().inc(CTag.FILE_BYTES_READ, mrgSegmet.size-approxInMemSize);
					//	            story.getCounters().inc(CounterTag.FILE_BYTES_WRITTEN, mrgSegmet.size);
					//	            story.getCounters().inc(CounterTag.SPILLED_RECORDS, mrgSegmet.records);


				}

				this.clear();
				////added by suhel may need to delete later  
				segments.add(mrgSegmet);
				numSegments = segments.size();
				Collections.sort(segments, segmentComparator);
				//end added by suhel
				return mrgSegmet;

			} else {

				mlog.info("Merging " + segmentsToMerge.size() + 
						" intermediate segments out of a total of " + 
						(segments.size()+segmentsToMerge.size()));
				//we want to spread the creation of temp files on multiple disks if 
				//available under the space constraints
				double approxOutputSize = 0; 
				double approxOutputRecord = 0; 

				double approxInMemSize = 0;
				double approxInMemRecord = 0;

				for (Datum s : segmentsToMerge) {
					approxOutputSize += s.size * (1+ CHKSUM_AS_FRACTION );
					approxOutputRecord+= s.records;

					Type type=(Type)s.getData();
					if(type == Type.hard_mem){
						approxInMemRecord+=s.records;
						approxInMemSize+=s.size;
					}
				}

				Datum mrgSegmet = new Datum("m_" + segments.size(),
						approxOutputSize, approxOutputRecord);
				mrgSegmet.setData(Type.hard_hard);
				//mlog.info("approx size "+ approxOutputSize);

				{

					hdd.read(mrgSegmet.size-approxInMemSize,task, HTAG.merge_read.id(), mrgSegmet);
					hdd.write(mrgSegmet.size,task, HTAG.merg_write.id(),mrgSegmet);


					Datum.collect(task, HTAG.merge_read.id(),HTAG.merg_write.id());


					story.getCounters().inc(CTag.FILE_BYTES_READ, mrgSegmet.size-approxInMemSize);
					story.getCounters().inc(CTag.FILE_BYTES_WRITTEN, mrgSegmet.size);
					story.getCounters().inc(CTag.SPILLED_RECORDS, mrgSegmet.records);



				}

				//TODO try to write files here writeFile(this, writer, reporter, conf);
				//we finished one single level merge; now clean up the priority 
				//queue
				this.clear();


				segments.add(mrgSegmet);
				numSegments = segments.size();
				Collections.sort(segments, segmentComparator);

				passNo++;
			}
			//we are worried about only the first pass merge factor. So reset the 
			//factor to what it originally was
			factor = origFactor;
		} while(true);

	}
	public Datum merge(int factor,int inMem,Sim_entity operator,
			HLogger mlog, HTaskTracker tracker, HCounter counter, HCombiner combiner){
		
		HDD hdd =tracker.getHdd();
		CPU cpu=tracker.getCpu();
		
		int localInMem=0;
		for (Datum seg : segments) {
			if (seg.isInMemory())
				localInMem++;
		}
		assert localInMem==inMem;

		mlog.info("Merging " + segments.size() + " sorted segments. inMem="+inMem);

		//create the MergeStreams from the sorted map created in the constructor
		//and dump the final output to a file
		int numSegments = segments.size();
		int origFactor = factor;
		int passNo = 1;
		do {
			//get the factor for this pass of merge. We assume in-memory segments
			//are the first entries in the segment list and that the pass factor
			//doesn't apply to them
			factor = getPassFactor(factor, passNo, numSegments - inMem);
			if (1 == passNo) {
				factor += inMem;
				//TODO check inMem later
				inMem=0;
			}
			List<Datum> segmentsToMerge =new ArrayList<Datum>();
			int segmentsConsidered = 0;
			int numSegmentsToConsider = factor;
			long startBytes = 0; // starting bytes of segments of this merge
			while (true) {
				//extract the smallest 'factor' number of segments  
				//Call cleanup on the empty segments (no key/value data)
				List<Datum> mStream = 
					getSegmentDescriptors(numSegmentsToConsider, segments);

				for ( int i=0; i< mStream.size(); i++) {
					Datum segment = mStream.get(i);
					// Initialize the segment at the last possible moment;
					// this helps in ensuring we don't use buffers until we need them
					// segment.init(readsCounter);
					boolean hasNext = i < mStream.size();
					startBytes += segment.size;

					if (hasNext) {
						segmentsToMerge.add(segment);
						segmentsConsidered++;
					}
					else {
						numSegments--; //we ignore this segment for the merge
					}
				}
				//if we have the desired number of segments
				//or looked at all available segments, we break
				if (segmentsConsidered == factor || 
						segments.size() == 0) {
					break;
				}

				numSegmentsToConsider = factor - segmentsConsidered;
			}

			//feed the streams to the priority queue
			clear();
			for (Datum segment : segmentsToMerge) {
				put(segment);
				//            readCounter += segment.size;

			}

			//if we have lesser number of segments remaining, then just return the
			//iterator, else do another single level merge
			if (numSegments <= factor) {
				// Reset totalBytesProcessed to track the progress of the final merge.
				// This is considered the progress of the reducePhase, the 3rd phase
				// of reduce task. Currently totalBytesProcessed is not used in sort
				// phase of reduce task(i.e. when intermediate merges happen).
				totalBytesProcessed = startBytes;
				//calculate the length of the remaining segments. Required for 
				//calculating the merge progress
				double totalBytes = 0;

				for (int i = 0; i < segmentsToMerge.size(); i++) {
					totalBytes += segmentsToMerge.get(i).size;
				}


				if (totalBytes != 0) //being paranoid
				progPerByte = 1.0f / (float)totalBytes;

				if (totalBytes != 0)
					mergeProgress.set(totalBytesProcessed * progPerByte);
				else
					mergeProgress.set(1.0f); // Last pass and no segments left - we're done

				mlog.info("Down to the last merge-pass, with " + numSegments + 
						" segments left of total size: " + totalBytes + " bytes");

				//            
				double approxOutputSize = 0;
				double approxOutputRecord =0;

				double approxInMemSize = 0;
				double approxInMemRecord = 0;

				double approxOrgRecord = 0;
				
				
				for (Datum s : segmentsToMerge) {
					approxOutputSize += s.size * (1 + CHKSUM_AS_FRACTION);
					approxOutputRecord += s.records;

					approxOrgRecord += s.orgRecords;
					if (s.isInMemory()) {
						approxInMemRecord += s.records;
						approxInMemSize += s.size;
					}

				}
				Datum mrgSegmet = new Datum("m_" + segments.size(),
						approxOutputSize, approxOutputRecord);
				mrgSegmet.orgRecords = approxOrgRecord;// do not forget

				
				if( combiner != null){
					mlog.info("last combine pass combine input "+mrgSegmet.records );

					counter.inc(CTag.COMBINE_INPUT_RECORDS, mrgSegmet.records);
					mrgSegmet = combiner.combine(mrgSegmet);
					counter.inc(CTag.COMBINE_OUTPUT_RECORDS, mrgSegmet.records);
					/*cpu combine cost*/
					cpu.work( combiner.cost(mrgSegmet),
							operator.get_id(), HTAG.combine_with_cpu.id, mrgSegmet);
					Datum.collectOne(operator, HTAG.combine_with_cpu.id);
					

					mlog.info("last combine pass combine output "+mrgSegmet.records );

				}

				mrgSegmet.setInMemory(true);
				//mlog.info("approx size "+ approxOutputSize);
				
				double sizeToRead=approxOutputSize-approxInMemSize;
				
				//read
				if(sizeToRead>0){
					hdd.read(sizeToRead,operator, HTAG.merge_read.id(), mrgSegmet);
					Datum.collectOne(operator, HTAG.merge_read.id());
					counter.inc(CTag.FILE_BYTES_READ, sizeToRead);
				}

				mlog.info("total bytes proccessed "+ mrgSegmet.size);

				this.clear();

				////added by suhel may need to delete later  
				segments.add(mrgSegmet);
				numSegments = segments.size();
				Collections.sort(segments, segmentComparator);
				//end added by suhel
				return mrgSegmet;
			} else {

				//intermediate merge
				mlog.info("Merging " + segmentsToMerge.size() + 
						" intermediate segments out of a total of " + 
						(segments.size()+segmentsToMerge.size()));
				//we want to spread the creation of temp files on multiple disks if 
				//available under the space constraints
				double approxOutputSize = 0;
				double approxOutputRecord =0;

				double approxInMemSize = 0;
				double approxInMemRecord = 0;
				
				double approxOrgRecord = 0;



				for (Datum s : segmentsToMerge) {
					approxOutputSize += s.size * (1+ CHKSUM_AS_FRACTION );
					approxOutputRecord+= s.records;
					
					approxOrgRecord += s.orgRecords;

					if(s.isInMemory()){
						approxInMemRecord+=s.records;
						approxInMemSize+=s.size;
					}

				}
				Datum mrgSegmet = new Datum("m_" + segments.size(),
						approxOutputSize, approxOutputRecord);
					mrgSegmet.orgRecords = approxOrgRecord;//do not forget
				mrgSegmet.setInMemory(false);
				
				if( combiner != null){
					mlog.info("intermadiate combine pass combine input "+mrgSegmet.records );

					counter.inc(CTag.COMBINE_INPUT_RECORDS, mrgSegmet.records);
					mrgSegmet = combiner.combine(mrgSegmet);
					counter.inc(CTag.COMBINE_OUTPUT_RECORDS, mrgSegmet.records);

					/*cpu combine cost*/
					cpu.work( combiner.cost(mrgSegmet),
							operator.get_id(), HTAG.combine_with_cpu.id, mrgSegmet);
					Datum.collectOne(operator, HTAG.combine_with_cpu.id);
					
					mlog.info("intermadiate combine pass combine out "+mrgSegmet.records );

				}
				//mlog.info("approx size "+ approxOutputSize);

				double sizeToRead=approxOutputSize-approxInMemSize;
				assert sizeToRead>0;

				{
						
						hdd.read( sizeToRead,operator , HTAG.merge_read.id(), mrgSegmet);
						hdd.write(mrgSegmet.size, operator, HTAG.merg_write.id(),mrgSegmet);//modified
						Datum.collect(operator, HTAG.merge_read.id(),HTAG.merg_write.id());
						
						counter.inc(CTag.SPILLED_RECORDS, mrgSegmet.records);
						counter.inc(CTag.FILE_BYTES_WRITTEN, mrgSegmet.size);
						
						counter.inc(CTag.FILE_BYTES_READ, sizeToRead);
				
				}
				//TODO try to write files here writeFile(this, writer, reporter, conf);
				//we finished one single level merge; now clean up the priority 
				//queue
				this.clear();


				segments.add(mrgSegmet);
				numSegments = segments.size();
				Collections.sort(segments, segmentComparator);

				passNo++;
			}
			//we are worried about only the first pass merge factor. So reset the 
			//factor to what it originally was
			factor = origFactor;
		} while(true);

	}

	public static Datum mergeToMem(int factor,int inMem,Sim_entity operator,
			HLogger mlog, HTaskTracker tracker, HCounter counter, Collection<Datum> segments, HCombiner combiner){
		HMergeQueue queu=new HMergeQueue(segments);
		Datum outmrg=queu.merge(factor, inMem, operator, mlog, tracker, counter, combiner);
		
		outmrg.setInMemory(true);
		return outmrg;
	}


	public static Datum mergeToHard(int factor,int inMem,Sim_entity entity,
			HLogger mlog, HTaskTracker tracker, HCounter counter, Collection<Datum> segments, HCombiner combiner){
		
		HMergeQueue queu=new HMergeQueue(segments);
		
		Datum outmrg=queu.merge(factor, inMem, entity, mlog, tracker, counter, combiner);
			
		HDD hdd = tracker.getHdd();
		hdd.write(outmrg.size, entity, HTAG.merg_write.id(), outmrg);
		
		Datum outmrgReturn=(Datum) Datum.collectOne(entity, HTAG.merg_write.id());
		
		assert outmrg==outmrgReturn;
		
		counter.inc(CTag.SPILLED_RECORDS, outmrg.records);
		counter.inc(CTag.FILE_BYTES_WRITTEN, outmrg.size);
		
		outmrg.setInMemory(false);
		return outmrg;
	}


	/**
	 * Determine the number of segments to merge in a given pass. Assuming more
	 * than factor segments, the first pass should attempt to bring the total
	 * number of segments - 1 to be divisible by the factor - 1 (each pass
	 * takes X segments and produces 1) to minimize the number of merges.
	 */
	private int getPassFactor(int factor, int passNo, int numSegments) {
		if (passNo > 1 || numSegments <= factor || factor == 1) 
			return factor;
		int mod = (numSegments - 1) % (factor - 1);
		if (mod == 0)
			return factor;
		return mod + 1;
	}

	/** Return (& remove) the requested number of segment descriptors from the
	 * sorted map.
	 */
	private static List<Datum> getSegmentDescriptors(int numDescriptors, List<Datum> segments) {
		if (numDescriptors > segments.size()) {
			List<Datum> subList = new ArrayList<Datum>(segments);
			segments.clear();
			return subList;
		}

		List<Datum> subList = 
			new ArrayList<Datum>(segments.subList(0, numDescriptors));
		for (int i=0; i < numDescriptors; ++i) {
			segments.remove(0);
		}
		return subList;
	}

	public HProgress getProgress() {
		return mergeProgress;
	}


}




interface HIterator{
	boolean next() throws IOException;

	/** Gets the Progress object; this has a float (0.0 - 1.0) 
	 * indicating the bytes processed by the iterator so far
	 */
	HProgress getProgress();
}



class HProgress{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HProgress.class);

	double progerss=0;
	public void set(float f) {
		this.progerss=f;

	}
	public void setStatus(String string) {
		// TODO Auto-generated method stub

	}

}