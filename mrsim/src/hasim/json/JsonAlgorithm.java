package hasim.json;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonAlgorithm {

	private double mapCost = 10.0;
	private double mapSize = 5.769283;
	private double mapRecords = 1.0;
	private double mapOutAvRecordSize=88.0;
	
	private double combineCost = 1.0;
	private double combineSize = 1.0;//could be combineGroups
	private double combineRecords = 1.0;
	
	private double combineGroups=1.0;
	private double combineOutAvRecordSize=1.0;
	private double combineOutAvRecordSize_add=0.0;
	
	private double reduceCost = 1.0;
	private double reduceRecords = 0.000000115;
	private double reduceOutAvRecordSize=91.875;
	
	
	private double compressionCost=1.0;
	private double uncompressionCost=1.0;
	private double compressionSize=1.0;
	
	public double getCompressionCost() {
		return compressionCost;
	}
	public void setCompressionCost(double compressionCost) {
		this.compressionCost = compressionCost;
	}
	public double getUncompressionCost() {
		return uncompressionCost;
	}
	public void setUncompressionCost(double uncompressionCost) {
		this.uncompressionCost = uncompressionCost;
	}
	public double getCompressionSize() {
		return compressionSize;
	}
	public void setCompressionSize(double compressionSize) {
		this.compressionSize = compressionSize;
	}


	public double getMapCost() {
		return mapCost;
	}
	public void setMapCost(double mapCost) {
		this.mapCost = mapCost;
	}
	public double getMapSize() {
		return mapSize;
	}
	public void setMapSize(double mapSize) {
		this.mapSize = mapSize;
	}
	public double getMapRecords() {
		return mapRecords;
	}
	public void setMapRecords(double mapRecords) {
		this.mapRecords = mapRecords;
	}
	public double getCombineCost() {
		return combineCost;
	}
	public void setCombineCost(double combineCost) {
		this.combineCost = combineCost;
	}
	public double getCombineSize() {
		return combineSize;
	}
	public void setCombineSize(double combineSize) {
		this.combineSize = combineSize;
	}
	public double getCombineRecords() {
		return combineRecords;
	}
	public void setCombineRecords(double combineRecords) {
		this.combineRecords = combineRecords;
	}
	
	
	public double getCombineGroups() {
		return combineGroups;
	}
	public void setCombineGroups(double combineGroups) {
		this.combineGroups = combineGroups;
	}
	
	
	public double getCombineOutAvRecordSize() {
		return combineOutAvRecordSize;
	}
	public void setCombineOutAvRecordSize(double combineOutAvRecordSize) {
		this.combineOutAvRecordSize = combineOutAvRecordSize;
	}
	public double getCombineOutAvRecordSize_add() {
		return combineOutAvRecordSize_add;
	}
	public void setCombineOutAvRecordSize_add(double combineOutAvRecordSizeAdd) {
		combineOutAvRecordSize_add = combineOutAvRecordSizeAdd;
	}
	public double getReduceCost() {
		return reduceCost;
	}
	public void setReduceCost(double reduceCost) {
		this.reduceCost = reduceCost;
	}
	

	public double getReduceRecords() {
		return reduceRecords;
	}
	public void setReduceRecords(double reduceRecords) {
		this.reduceRecords = reduceRecords;
	}
	
	
	public double getMapOutAvRecordSize() {
		return mapOutAvRecordSize;
	}
	public void setMapOutAvRecordSize(double mapOutAvRecordSize) {
		this.mapOutAvRecordSize = mapOutAvRecordSize;
	}
	

	public static JsonAlgorithm read(String filename, Class cls)		
		throws Exception{
		ObjectMapper mapper=new ObjectMapper();
		JsonAlgorithm alg=mapper.readValue(filename, JsonAlgorithm.class);
		return alg;
	}
	public static Map<String, JsonAlgorithm> readFrom(String filename) 
		throws Exception{
		ObjectMapper mapper=new ObjectMapper();
		Map<String, JsonAlgorithm> amap = mapper.readValue(new File("data/json/algorithm.json"), 
				new TypeReference<Map<String, JsonAlgorithm>>() {});
		return amap;
	}
	
	
	public double getReduceOutAvRecordSize() {
		return reduceOutAvRecordSize;
	}
	public void setReduceOutAvRecordSize(double reduceOutAvRecordSize) {
		this.reduceOutAvRecordSize = reduceOutAvRecordSize;
	}
	public static void main(String[] args) throws Exception{
		JsonAlgorithm alg=JsonJob.read("data/json/alg.json", JsonAlgorithm.class);
		System.out.println(alg);
		
		
		
	
	}
		
	
	@Override
	public String toString(){
		String result="";
		try {
			Class cls=Class.forName(this.getClass().getName());
			Field[] flds= cls.getDeclaredFields();
			for (Field fld : flds) 
				result+= "\n"+fld.getName()+":"+fld.get(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
