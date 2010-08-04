package hasim.json;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ser.ToStringSerializer;
import org.codehaus.jackson.type.TypeReference;



public class JsonJob {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JsonJob.class);

	final public static double REF_SIZE=16;
	public static double getRefSize() {
		return REF_SIZE;
	}
	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper=new ObjectMapper();
		
		JsonJob job=mapper.readValue(new File("data/json/all/job.json"),
				JsonJob.class);
		
		save("data/suhel.json", job);
		System.out.println(job);
//		mapper.writeValue(new File("data/json/out/job_out.txt"), job);
//
//		System.out.println(job);

	}
	
	@SuppressWarnings("unchecked")
	public static <E> E read(String filename, Class<E> cls){
		E result=null;

		try {

			if(! new File(filename).exists()){
				System.err.println("error file "+filename+" not found");
				throw new IOException();
			}
			ObjectMapper mapper=new ObjectMapper();

			result=	mapper.readValue(new File(filename),cls);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	public static JsonConfig readFrom(String filename)throws Exception{
		if(! new File(filename).exists()){
			System.err.println("file "+filename+" not found");
			throw new IOException();
		}
		ObjectMapper mapper=new ObjectMapper();

		JsonConfig result=
			mapper.readValue(new File(filename),
					JsonConfig.class);

		return result;
	}
	public static <K, V> Map<K,V> readMap(String filename, Class<K> k, Class<V> v){
		ObjectMapper mapper=new ObjectMapper();
		Map<K, V> result = null;
		try {
			result = mapper.readValue(new File(filename), 
					new TypeReference<Map<K, V>>() {});
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static void save(String jsonFile, Object value){
		ObjectMapper mapper=new ObjectMapper();
		try {
			mapper.writeValue(new File(jsonFile), value);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	private JsonAlgorithm algorithm=new JsonAlgorithm();

	private JsonDatum data=null;//default is null
	private List<JsonDatum> inputSplits= new ArrayList<JsonDatum>();
	/*
	 * Reducers
	 */
	private int ioSortFactor=10;//max number of spills to merge at the same type

	private int replication=1;

	public int getReplication() {
		return replication;
	}
	public void setReplication(int replication) {
		this.replication = replication;
	}

	/*
	 * Mappers
	 */
	private double ioSortMb=100;
	private double ioSortRecordPercent=0.05;
	
	private double ioSortSpillPercent=0.80;
	private String jobName="job 1";
	/**
	 * memory heap in MB
	 */
	private int mapredChildJavaOpts=200;
	
	private int mapredInmemMergeThreshold=1000;
	private double mapredJobReduceInputBufferPercent=0.0;
	private double mapredJobShuffleInputBufferPercent=0.70;
	
	private double mapredJobShuffleMergePercent=0.66;
	
	private int mapReduceParallelCopies=5;
	private double memoryLimit=-1;
	private int numberOfMappers=4,numberOfReducers=2;
	private List<JsonDatum> outputSplits= new ArrayList<JsonDatum>();
	
	private boolean useCombiner=false;
	private boolean useCompression=false;

	public JsonAlgorithm getAlgorithm() {
		return algorithm;
	}
	public JsonDatum getData() {
		return data;
	}
	public List<JsonDatum> getInputSplits() {
		return inputSplits;
	}
	public int getInt(String s, int defint){
		return defint;
	}
	public int getIoSortFactor() {
		return ioSortFactor;
	}
	public double getIoSortMb() {
		return ioSortMb;
	}
	public double getIoSortRecordPercent() {
		return ioSortRecordPercent;
	}
	public double getIoSortSpillPercent() {
		return ioSortSpillPercent;
	}
	public String getJobName() {
		return jobName;
	}
	public int getMapredChildJavaOpts() {
		return mapredChildJavaOpts;
	}
	public int getMapredInmemMergeThreshold() {
		return mapredInmemMergeThreshold;
	}
	public double getMapredJobReduceInputBufferPercent() {
		return mapredJobReduceInputBufferPercent;
	}
	public double getMapredJobShuffleInputBufferPercent() {
		return mapredJobShuffleInputBufferPercent;
	}
	public double getMapredJobShuffleMergePercent() {
		return mapredJobShuffleMergePercent;
	}
	
	public int getMapReduceParallelCopies() {
		return mapReduceParallelCopies;
	}

	public double getMemoryLimit() {
		return memoryLimit;
	}

	public int getNumberOfMappers() {
		return numberOfMappers;
	}

	public int getNumberOfReducers() {
		return numberOfReducers;
	}

	
	public List<JsonDatum> getOutputSplits() {
		return outputSplits;
	}

	public boolean isUseCombiner() {
		return useCombiner;
	}

	
	public boolean isUseCompression() {
		return useCompression;
	}

	public void setAlgorithm(JsonAlgorithm algorithm) {
		this.algorithm = algorithm;
	}

	public void setData(JsonDatum datat) {
		this.data = datat;
	}

	public void setInputSplits(List<JsonDatum> inputSplits) {
		this.inputSplits = inputSplits;
	}

	public void setIoSortFactor(int ioSortFactor) {
		this.ioSortFactor = ioSortFactor;
	}

	public void setIoSortMb(double ioSortMb) {
		this.ioSortMb = ioSortMb;
	}

	public void setIoSortRecordPercent(double ioSortRecordPercent) {
		this.ioSortRecordPercent = ioSortRecordPercent;
	}

	public void setIoSortSpillPercent(double ioSortSpillPercent) {
		this.ioSortSpillPercent = ioSortSpillPercent;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setMapredChildJavaOpts(int mapredChildJavaOpts) {
		this.mapredChildJavaOpts = mapredChildJavaOpts;
	}
	
	
	public void setMapredInmemMergeThreshold(int mapredInmemMergeThreshold) {
		this.mapredInmemMergeThreshold = mapredInmemMergeThreshold;
	}
	public void setMapredJobReduceInputBufferPercent(
			double mapredJobReduceInputBufferPercent) {
		this.mapredJobReduceInputBufferPercent = mapredJobReduceInputBufferPercent;
	}
	
	
	public void setMapredJobShuffleInputBufferPercent(
			double mapredJobShuffleInputBufferPercent) {
		this.mapredJobShuffleInputBufferPercent = mapredJobShuffleInputBufferPercent;
	}
	public void setMapredJobShuffleMergePercent(double mapredJobShuffleMergePercent) {
		this.mapredJobShuffleMergePercent = mapredJobShuffleMergePercent;
	}
	public void setMapReduceParallelCopies(int mapReduceParallelCopies) {
		this.mapReduceParallelCopies = mapReduceParallelCopies;
	}

	
	public void setMemoryLimit(double memeoryLimit) {
		this.memoryLimit = memeoryLimit;
	}
	public void setNumberOfMappers(int numberOfMappers) {
		this.numberOfMappers = numberOfMappers;
	}
	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
	
	public void setOutputSplits(List<JsonDatum> outputSplits) {
		this.outputSplits = outputSplits;
	}

	public void setUseCombiner(boolean useCombiner) {
		this.useCombiner = useCombiner;
	}

	public void setUseCompression(boolean useCompression) {
		this.useCompression = useCompression;
	}
	private void test(String name){
		try {
		
			Set<Integer> set=(Set<Integer>) Class.forName(name).newInstance();
			System.out.println(set);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	
	@Override
	public String toString(){
		String result="";
		try {
			Class cls=Class.forName(this.getClass().getName());
			Field[] flds= cls.getDeclaredFields();
			for (Field fld : flds) 
				result+= "\n"+fld.getName()+":{"+fld.get(this)+"}";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	public String toStringConfig(){
		String result="";
		try {
			Class cls=Class.forName(this.getClass().getName());
			Field[] flds= cls.getDeclaredFields();
			for (Field fld : flds) {
				if(fld.getName().startsWith("inputSplits"))continue;
				if(fld.getName().startsWith("outputSplits"))continue;
				result+= "\n"+fld.getName()+":{"+fld.get(this)+"}";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
