package hasim;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream.GetField;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


public enum RF {
	 hlogs, jobs, mappers, reducers, machines,graph, jsonJob,jsonRack, jsonConfig;

	static Map<RF, String> map = new LinkedHashMap<RF, String>();
//	static Set<RF> exclude=new LinkedHashSet<RF>();
	static {
		map.put(RF.hlogs, File.separator + "hlogs");
		map.put(RF.jobs, File.separator + "jobs");
		map.put(RF.mappers, File.separator + "mappers");
		map.put(RF.reducers, File.separator + "reducers");
		map.put(RF.machines,  File.separator + "machines");
		map.put(RF.graph, File.separator + "graph.sjg");

		map.put(RF.jsonJob, File.separator + "job.json");
		
		map.put(RF.jsonRack,File.separator + "jsonRack.json");

		map.put(RF.jsonConfig, File.separator + "jsonConfig.json");
		
		

	}
	

	public static String get(RF rf) {
		return map.get(rf);
	}
	public static String get(String dir,RF rf) {
		return dir+map.get(rf);
	}
	
	public static void mkdir(String dir, RF e){
		new File(dir+get(e)).mkdirs();
	}
	
	/*
	 * create new Job dir, return it
	 */
	public static String newJobDir(String dir, String job){
        assert new File(dir).exists();
        
        String result=dir+File.separator+job;
        assert ! new File(result).exists();
        
        new File(result).mkdir();
        return result;
	}
	public static String  newResultDir(String dirName){
		return newResultDir(dirName, "");
	}	
	public static String  newResultDir(String dirName, String newName){
        DateFormat df=DateFormat.getDateTimeInstance();
        File gfile=new File(dirName);
        if( !gfile.exists() || !gfile.isDirectory())
        	gfile.mkdirs();
        
        String filename=gfile.getAbsolutePath()+File.separator+
        	newName+ gfile.list().length+ "-"+df.format(new Date());
        
        new File(filename).mkdirs();
        new File(RF.get(filename, machines)).mkdir();
        new File(RF.get(filename, hlogs)).mkdir();
        return filename;
	}
	public static String getLastResultDir(String dirName){
        File gfile=new File(dirName);
        String[] filenames=gfile.list();
        Arrays.sort(filenames);        
        return gfile.getAbsolutePath()+File.separator+filenames[filenames.length-1];
	}
	
	public static void makeAllJob(String jobDir){
		mkdir(jobDir, hlogs);
		mkdir(jobDir, mappers);
		mkdir(jobDir, reducers);
	}
	
	public static String firstFileInDir(String dirName){
		return firstFileInDir(dirName, ".json");
	}
	public static String firstFileInDir(String dirName, String suffix){
		File file=new File(dirName);
		assert file.exists();
		assert file.isDirectory();
		assert file.list().length>0;
		
		for (File f : file.listFiles()) {
			if(f.getName().toLowerCase().endsWith(suffix)){
				return f.getAbsolutePath();
			}
		}
		return null;
	}
	public static void copy(String src, String dst)  {
		
		try {
			copy(new File(src), new File(dst));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	// Copies src file to dst file.
	// If the dst file does not exist, it is created
	public static void copy(File src, File dst) throws IOException   {
	    FileInputStream in = new FileInputStream(src);
	    FileOutputStream out = new FileOutputStream(dst);

	    // Transfer bytes from in to out
	    byte[] buf = new byte[1024];
	    int len;
	    while ((len = in.read(buf)) > 0) {
	        out.write(buf, 0, len);
	    }
	    in.close();
	    out.close();
	}
	
	public static void main(String[] args) throws IOException {
//		String d=newResultDir("rrr");
//		System.out.println(d);
//		String d2=getLastResultDir("rrr");
//		assert d.equals(d2);
//		System.out.println(d2);
//		System.out.println(newJobDir(d2, "myjob"));
		//		
//		makeAllJob(job);
//		copy("data/json/job.json", "results/file.json");
		System.out.println(firstFileInDir("data/json/rack"));
	}
	
	
};