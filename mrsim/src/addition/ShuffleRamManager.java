package addition;

import  static hasim.Tools.format; 
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;

import test.Mem;

import hasim.HLogger;
import hasim.Tools;
import hasim.json.JsonJob;

public class ShuffleRamManager {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ShuffleRamManager.class);

//	ReduceCopier copier;
	/* Maximum percentage of the in-memory limit that a single shuffle can 
	 * consume*/ 
	private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;

	/* Maximum percentage of shuffle-threads which can be stalled 
	 * simultaneously after which a merge is triggered. */ 
	private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;

	final HLogger hlog;
	/**
	 * Usage threshold for in-memory output accumulation.
	 */
	final double maxInMemCopyPer;
	
	/**
	 * When we accumulate maxInMemOutputs number of files in ram, we merge/spill
	 */
	final int maxInMemOutputs;

	public AtomicBoolean isMerging=new AtomicBoolean(false);
	

	public void setIsMerging(AtomicBoolean isMerging) {
		this.isMerging = isMerging;
	}

	private final double maxSize;
	private final double maxSingleShuffleLimit;
	private final int numCopiers;
	final double maxInMemCopyUse;

	private double size = 0;

	private double fullSize = 0;
	private int numPendingRequests = 0;

	public int incNumPendingRequests() {
		numPendingRequests++;
		return numPendingRequests;
	}
	public int decNumPendingRequests() {
		numPendingRequests--;
		return numPendingRequests;
	}
	
	public int getNumPendingRequests() {
		return numPendingRequests;
	}

	public void setNumPendingRequests(int numPendingRequests) {
		this.numPendingRequests = numPendingRequests;
	}

	private int numRequiredMapOutputs = 0;
	private int numClosed = 0;
	private boolean closed = false;

//	public void reset(){
//		size=0;
//		fullSize=0;
////		numPendingRequests=0;
////		numRequiredMapOutputs=0;
//		numClosed=0;
//		closed=false;
//	}
	
	public ShuffleRamManager(JsonJob conf, int numCopiers, HLogger hlog){
		// ("mapred.job.shuffle.input.buffer.percent", 0.70f);
		maxInMemCopyUse =
			conf.getMapredJobShuffleInputBufferPercent();
		assert maxInMemCopyUse <=1.0 && maxInMemCopyUse >=0.0;

		// conf.getFloat("mapred.job.shuffle.merge.percent", 0.66f);
		this.maxInMemCopyPer = conf.getMapredJobShuffleMergePercent();
		
		// getInt("mapred.inmem.merge.threshold", 1000);
//		this.maxInMemOutputs = conf.getMapredInmemMergeThreshold();
		this.maxInMemOutputs = conf.getMapredInmemMergeThreshold()-1;//by suhel

		
		//				maxSize = conf.getMemeoryLimit();
		if(conf.getMemoryLimit()>0){
			maxSize=conf.getMemoryLimit();
		}else{
			maxSize = conf.getMapredChildJavaOpts() * maxInMemCopyUse
			* (1024*1024);
			logger.info("memory limit = "+ maxSize);
			logger.info("memory MEM = "+ Mem.calc(conf.getMapredChildJavaOpts()));

		}

		maxSingleShuffleLimit = maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION;
		
		this.numCopiers=numCopiers;
		
		assert hlog != null;
		this.hlog=hlog;

		hlog.info("ShuffleRamManager: MemoryLimit=" + format(maxSize) + 
				", MaxSingleShuffleLimit=" + format(maxSingleShuffleLimit));
//		logger.info("ShuffleRamManager: MemoryLimit=" + format(maxSize) + 
//				", MaxSingleShuffleLimit=" + format(maxSingleShuffleLimit));
	}

	public synchronized boolean canReserve(double requestedSize) {
		return (size+requestedSize)<= maxSize;
	}

	public synchronized boolean isToMerge() {
		return getPercentUsed() >= maxInMemCopyPer && numClosed >= 2 ;
	}

	public synchronized boolean reserve(double requestedSize) {

		// Wait till the request can be fulfilled...
		boolean result=(size + requestedSize) <= maxSize;
		if (result) {
			size += requestedSize;
		}
		hlog.info("MEMORY reserve: "+ requestedSize+", "+result+ ", "+toString());
		return result;
	}

	public void reset(){
//		size=0;
		hlog.info("MEMORY RESET "+ toString());
		assert fullSize< 1e-5;
		assert numClosed==0;
		fullSize=0;
		numClosed=0;
	}
	public synchronized void unreserve(double requestedSize) {

		size -= requestedSize;

		fullSize -= requestedSize;
		--numClosed;
//		hlog.info("MEMORY unreserve: "+ requestedSize+", "+toString());
	}
	
	/**
	 * 
	 * @return false: no merge, true: start merging
	 *
	 */
	public boolean waitForDataToMerge()  {
		

		// Start in-memory merge if manager has been closed or...
		boolean cond0=!closed;
		// In-memory threshold exceeded and at least two segments
		// have been fetched
		boolean cond1=(getPercentUsed() < maxInMemCopyPer || numClosed < 2);

		// More than "mapred.inmem.merge.threshold" map outputs
				// have been fetched into memory
		boolean cond2=(maxInMemOutputs <= 0 || numClosed < maxInMemOutputs);
		
		// More than MAX... threads are blocked on the RamManager
		// or the blocked threads are the last map outputs to be
		// fetched. If numRequiredMapOutputs is zero, either
		// setNumCopiedMapOutputs has not been called (no map ouputs
		// have been fetched, so there is nothing to merge) or the
		// last map outputs being transferred without
		// contention, so a merge would be premature.
		boolean cond3=((numPendingRequests < 
				numCopiers*MAX_STALLED_SHUFFLE_THREADS_FRACTION -1 ||
				numClosed < 3) && 
				(0 == numRequiredMapOutputs ||
						numPendingRequests < numRequiredMapOutputs));
		
		
		boolean result=cond0 && cond1 	&& 	cond2 && cond3;

		hlog.info("waitformerge: "+cond0+", "+cond1+", "+ cond2+", "+cond3+" = "+result);
		hlog.info("pending = "+numPendingRequests );
		if (result ) {
			return true;
		}else
			return false;
//		done = closed;
//		return done;
	}

	public void closeInMemoryFile(double requestedSize) {
		fullSize += requestedSize;
		++numClosed;
	}

	public void setNumRequiredMapOutputs(int numRequiredMapOutputs) {
		this.numRequiredMapOutputs = numRequiredMapOutputs;
	}

	public void close() {
		closed = true;
		hlog.info("Closed ram manager");

	}

	public double getPercentUsed() {
		return fullSize/maxSize;
	}

	public double getPercentSize() {
		return size/maxSize;
	}

	double getMemoryLimit() {
		return maxSize;
	}

	public boolean canFitInMemory(double requestedSize) {
		return (requestedSize < Double.MAX_VALUE && 
				requestedSize < maxSingleShuffleLimit);
	}


	@Override
	public String toString() {
		String s= "RAM manager, pending:"+ numPendingRequests+
		", size:"+format(size)+", % "+format(getPercentSize())+
		", fullsize:"+format(fullSize)+", % "+format(getPercentUsed())+
		", numClosed:"+numClosed;
		return s;
	}

}