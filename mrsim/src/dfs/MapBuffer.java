package dfs;

import org.apache.log4j.Logger;

import hasim.json.JsonAlgorithm;
import hasim.json.JsonJob;

public class MapBuffer{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MapBuffer.class);

	final public static double RECSIZE = 16;

	public final double dataBuffer;
	public final double dataBufferLimit;
	public final double recordBuffer;
	public final double recordBufferLimit;
	public MapBuffer(double dataBufferLimit,double dataBuffer, 
			 double recordBufferLimit, double recordBuffer) {
		this.dataBuffer = dataBuffer;
		this.dataBufferLimit = dataBufferLimit;
		this.recordBuffer = recordBuffer;
		this.recordBufferLimit = recordBufferLimit;
	}
	
	@Override
	public String toString() {
		String result="data buffer = "+dataBufferLimit +" / "+ dataBuffer;
		result+= " , record buffer = "+recordBufferLimit +" / "+ recordBuffer;
		return result;
	}
	
	public static MapBuffer createMapBuffer( JsonJob job){
		
		// spill accounting
		double softRecordLimit;
		double softBufferLimit;



		//sanity checks
		double spillper = job.getIoSortSpillPercent();//("io.sort.spill.percent",(float)0.8);
		double recper = job.getIoSortRecordPercent();//("io.sort.record.percent",(float)0.05);
		double sortmb = job.getIoSortMb();//getInt("io.sort.mb", 100);
		// buffers and accounting
		double maxMemUsage = ((int)sortmb << 20) + 0.0;

		double recordCapacity = (maxMemUsage * recper);//size
//		{//correction
//			int correction=(int)recordCapacity;
//			correction -= correction % RECSIZE;
//			recordCapacity=correction;
//		}
		double dataCapacity = maxMemUsage - recordCapacity;//size

		recordCapacity /= RECSIZE;


		softBufferLimit = dataCapacity * spillper;

		softRecordLimit = recordCapacity * spillper ;
		
		
		MapBuffer mb=new MapBuffer(softBufferLimit, dataCapacity,
				softRecordLimit, recordCapacity);
		
		return mb;
	}
	
	public double getSpillRecords(double avRecordSize){
		
		double dataRecord= dataBufferLimit/(avRecordSize);
		double result= dataRecord > recordBufferLimit? 
				recordBufferLimit:dataRecord;
		
		return result;
	}
	
//	public double getSpillSize(double avRecordSize){
//		double spillRecords=getSpillRecords(avRecordSize);
//		return spillRecords * (avRecordSize+ REF_2);
//	}
}
