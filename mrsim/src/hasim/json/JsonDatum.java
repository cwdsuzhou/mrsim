package hasim.json;

import org.apache.log4j.Logger;

import hasim.core.Datum;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class JsonDatum {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JsonDatum.class);

	public static void main(String[] args)throws Exception {
		JsonJob job=JsonJob.read("data/json/job.json", JsonJob.class);
		logger.info(job.getInputSplits().get(0));
	}
	private String name="data";
//	private String location="m1";
	
	private List<String> replica=new ArrayList<String>();
	
	private double size=9197822290.0,records=69424600.0;
	
	
//	public String getLocation(){
//		return location;
//	}



	public String getName() {
		return name;
	}



	public double getRecords() {
		return records;
	}



	public List<String> getReplica() {
		return replica;
	}



	public double getSize() {
		return size;
	}


//
//	public void setLocation(String location) {
//		this.location = location;
//	}


	public void setName(String name) {
		this.name = name;
	}



	public void setRecords(double records) {
		this.records = records;
	}


	public void setReplica(List<String> replica) {
		this.replica = replica;
	}
	

	public void setSize(double size) {
		this.size = size;
	}
	
//	public Datum datum(){
//		return new Datum(name,size, records);
//	}
	
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
