package hasim.json;

import org.apache.log4j.Logger;

import hasim.core.CPU;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonCpu {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(JsonCpu.class);

	private int _cores;
	private double _speed;

	public int getCores() {
		return _cores;
	}
	public void setCores(int cores) {
		this._cores = cores;
	}
	public double getSpeed() {
		return _speed;
	}
	public void setSpeed(double speed) {
		this._speed = speed;
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

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper=new ObjectMapper();

		Map<String, JsonCpu> m=
			mapper.readValue(new File("data/json/cpu.json"),
					new TypeReference<Map<String, JsonCpu>>() {});
		logger.info(m);
	}
	public JsonCpu copy() {
		JsonCpu result=new JsonCpu();
		result.setCores(this._cores);
		result.setSpeed(this._speed);
		return result;
	}
}
