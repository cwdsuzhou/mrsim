package hasim.json;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class JsonHardDisk {
	private double write;
	private double capacity;
	private double read;
	private double seekTime;
	private int slots=1;
	
	
	public int getSlots() {
		return slots;
	}



	public void setSlots(int slots) {
		this.slots = slots;
	}



	public double getWrite() {
		return write;
	}



	public void setWrite(double write) {
		this.write = write;
	}



	public double getCapacity() {
		return capacity;
	}



	public void setCapacity(double capacity) {
		this.capacity = capacity;
	}



	public double getRead() {
		return read;
	}



	public void setRead(double read) {
		this.read = read;
	}



	public double getSeekTime() {
		return seekTime;
	}



	public void setSeekTime(double seekTime) {
		this.seekTime = seekTime;
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


	

	public static void main(String[] args) throws Exception{
		JsonHardDisk disk=JsonJob.read("data/json/disk.json", JsonHardDisk.class);
		System.out.println(disk);
//		ObjectMapper mapper=new ObjectMapper();
//		Map<String, JsonHardDisk> map=mapper.readValue(new File("data/json/hardDisk.json"),
//				new TypeReference<Map<St"data/json/hardDisk.json"ring, JsonHardDisk>>() {});
//		System.out.println(map.toString());
		
	}
	
	public JsonHardDisk copy() {
		JsonHardDisk result=new JsonHardDisk();
		result.capacity=this.capacity;
		result.seekTime=this.seekTime;
		result.read=this.read;
		result.write=this.write;
		return result;
	}
}
