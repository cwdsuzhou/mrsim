package addition2;

import org.apache.log4j.Logger;

import dfs.MapBuffer;


import hasim.HLogger;
import hasim.RF;
import hasim.Tools;
import hasim.core.Datum;
import hasim.json.JsonJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;




public class CalcMapper  {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(CalcMapper.class);

//	public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;




	
	public static void main(String[] args) {
		String jobfile=RF.getLastResultDir("data/json/job");
		logger.info(jobfile);

		JsonJob job=JsonJob.read(jobfile, JsonJob.class);

		MapBuffer.createMapBuffer(job);
	}
}

