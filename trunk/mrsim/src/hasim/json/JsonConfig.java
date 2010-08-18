package hasim.json;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonConfig  {

	
	//private final static Logger logger = Logger.getLogger(JsonConfig.class);
	
	public static void main(String[] args) throws Exception {
		Object config1= JsonJob.read("data/json/config.json", JsonConfig.class);
		System.out.println(config1);
		JsonConfig config= JsonJob.readFrom("data/json/config.json");
		System.out.println(config);
		//logger.info("maxIM:"+ config.getMaxIM());
		//List<Integer> list=(List<Integer>)config.getMap().get("someList");
//		logger.info("list:"+ list);
		
	}
	
	
	private double delta;
	private  double heartbeat;
	
	private Map<String, Object> map;
	private  int maxIM;
	private double maxSplitSize;
	
	private  double propDelay;
	
	private double ioSortMb;
	private double ioSortSpillPercent;
	private double ioSortRecordPercent;
	private double ioSortFactor;
	
	
	public double getDelta() {
		return delta;
	}
	public double getHeartbeat() {
		return heartbeat;
	}

	public Map<String, Object> getMap() {
		return map;
	}

	public int getMaxIM() {
		return maxIM;
	}
	public double getMaxSplitSize() {
		return maxSplitSize;
	}


	public  double getPropDelay() {
		return propDelay;
	}

	

	public void setDelta(double delta) {
		this.delta = delta;
	}
	

	
	public void setHeartbeat(double heartbeat) {
		this.heartbeat = heartbeat;
	}
	public void setMap(Map<String, Object> map2) {
		map = map2;
	}
	
	



	public void setMaxIM(int maxIM) {
		this.maxIM = maxIM;
	}
	
	public void setMaxSplitSize(double maxSplitSize) {
		this.maxSplitSize = maxSplitSize;
	}
	public  void setPropDelay(double propDelay) {
		this.propDelay = propDelay;
	}
	
	
	public double getIoSortMb() {
		return ioSortMb;
	}

	public void setIoSortMb(double ioSortMb) {
		this.ioSortMb = ioSortMb;
	}

	
	
	
	public double getIoSortSpillPercent() {
		return ioSortSpillPercent;
	}

	public void setIoSortSpillPercent(double ioSortSpillPercent) {
		this.ioSortSpillPercent = ioSortSpillPercent;
	}
	
	

	public double getIoSortRecordPercent() {
		return ioSortRecordPercent;
	}

	public void setIoSortRecordPercent(double ioSortRecordPercent) {
		this.ioSortRecordPercent = ioSortRecordPercent;
	}

	public double getIoSortFactor() {
		return ioSortFactor;
	}

	public void setIoSortFactor(double ioSortFactor) {
		this.ioSortFactor = ioSortFactor;
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
