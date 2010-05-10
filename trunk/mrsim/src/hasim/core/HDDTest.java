package hasim.core;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import gridsim.GridSim;
import hasim.HTAG;
import static hasim.Tools.format;
public class HDDTest extends Sim_entity {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HDDTest.class);

	List<Datum> files=new ArrayList<Datum>();
	HDD hdd;
	
	public HDDTest(String name) throws Exception {
		super(name);
		init();
	}
	
	private void init(){
		Datum f1= new Datum(4,50);
		Datum f2= new Datum(500, 50);
		Datum f3= new Datum(500, 50);
		Datum f4= new Datum(200,50);
		
		files.add(f1);
		files.add(f2);
		files.add(f3);
		files.add(f4);
		
		logger.info(f1);
		logger.info(f2);
		logger.info(f3);
		logger.info(f4);
	}


	public void body() {
		
		
		
		double start=Sim_system.clock();
		double size =49242931.75*20;
		double sizeW=65741122.41*10;
//		sizeW=0;
		
		long performance=System.nanoTime();
		hdd.read(size, this, HTAG.test_1.id(), "msg");
		hdd.read(size, this, HTAG.test_3.id(), "msg");
		hdd.write(sizeW, this, HTAG.test_2.id(), "write");
		
		Datum.collect(this, HTAG.test_1.id, HTAG.test_3.id, HTAG.test_2.id);
		
		performance=System.nanoTime()-performance;
		
		System.out.println("performance = "+ performance);
//		Datum.collectOne(this, HTAG.all_done.id());
//		Datum.collectOne(this, HTAG.test_1.id());
//
//		Datum.collectOne(this, HTAG.test_2.id());
		
		double duration=Sim_system.clock()-start;

		System.out.println("time = "+ duration);
		double speed= size/duration;
		System.out.println("speed = "+ format(speed) );
		

		//		hdd.read(10, this, HTAG.test_2.id(), files.get(1));
//		hdd.read(5, this, HTAG.test_3.id(), files.get(2));
//		hdd.read(7, this, HTAG.test_4.id(), files.get(3));
//		
//		for (int i = 0; i < 4; i++) {
//			Object o=Datum.collectAny(this, HTAG.test_1.id(),HTAG.test_2.id(),
//					HTAG.test_3.id(),HTAG.test_4.id());
//			logger.info("recieve "+ o+ " at time:"+ Sim_system.clock());
//		}
		
		sim_pause(1000000000);
		hdd.read(3, this, HTAG.addMapOutputFilesOnDisk.id(), "null msg");
		while(Sim_system.running()){
			try {
				Thread.sleep(1000);
				sim_schedule(hdd.get_id(), 30.0, HTAG.heartbeat.id());
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
//		hdd.submit(null);
	}
	public static void main(String[] args) {
		
		try {

			Sim_system.initialise();
			
			HDD hdd=new HDD("hdd", "data/json/all/disk.json");
			HDD.setDELTA(10000.0);
			logger.info("disk:"+ hdd);
			
			HDDTest user=new HDDTest("user_1");
			user.hdd=hdd;

			Sim_system.run();
			
			logger.info("Finish Simulation");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Unwanted errors happen");
		}
	}

}
