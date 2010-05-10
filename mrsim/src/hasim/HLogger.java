package hasim;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

import eduni.simjava.Sim_system;

public class HLogger {
	
	private String logFile="data/hlog";
	public String getLogFile() {
		return logFile;
	}

	public void setLogFile(String f) {
		logFile = f;
	}

	private static int ID=0;
	enum HLogLevel{debug, info, out,error};
	private static HLogLevel level=HLogLevel.info;
	public static HLogLevel getLevel() {
		return level;
	}

	public static void setLevel(HLogLevel level) {
		HLogger.level = level;
	}

	final private int id;
	final private String name;
	private StringBuffer sb=new StringBuffer();
	final private DecimalFormat format; 
	
	public HLogger(String name){
		this.id=(ID++);
		this.name=name+"_"+id;
		sb.append("Name:"+name);
		
		format = new DecimalFormat();
		format.setDecimalSeparatorAlwaysShown(true);
		format.setGroupingSize(3);
		format.setMaximumFractionDigits(2);
		format.setMinimumFractionDigits(2);
	}

	public int getId(){
		return id;
	}
	public HLogger() {
		this("hlogger");
	}
	
	public String getName() {
		return name;
	}
	
	public void info(String msg, double time){
		txt(msg, time, HLogLevel.info);
	}
	
	public void info(String msg){
		info(msg, Sim_system.clock());
	}
	public void error(String msg){
		info("ERROR: "+msg);
	}
	
	public void infoCounter(String msg, HCounter counters){
		info(msg+ counters.toString("\n\t"));
	}
	
	public void debug(String msg){
		txt("DEBUG: "+msg, Sim_system.clock(), HLogLevel.debug);
	}
	public void txt(String msg, double time, HLogLevel level2){
		if(level2.ordinal() >= HLogger.level.ordinal()){
			sb.append("\n"+format.format(time)+"\t"+ msg);			
		}
	}
	
	@Override
	public String toString() {
		return sb.toString();
	}
	
	
	public void save() {
		save(logFile+File.separator+name);
	}
	
//	public void clear(){
//		sb=new StringBuffer("Name:"+name);
//	}
	public void save(String fName) {
				File file=new File(fName);
		if(! file.getParentFile().exists() || file.getParentFile().isFile()){
			file= new File("data/logger/"+name);
		}
		try {
		    BufferedWriter out = new BufferedWriter(new FileWriter(file));
		    out.write(sb.toString());
		    out.close();
		} catch (IOException e) {
		}
	}
	
	public static void main(String[] args) {
		HLogger hlog=new HLogger("test");
		hlog.info("test to add one line", 0.1);
		hlog.info("test to add another line");
		
		System.out.println(hlog);
		hlog.save();
		hlog.save("data/test/savetotest");
	}
}
