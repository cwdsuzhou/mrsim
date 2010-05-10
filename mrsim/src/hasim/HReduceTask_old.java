package hasim;
//
//import org.apache.log4j.Logger;
//
//
//import hasim.core.Datum;
//import hasim.json.JsonConfig;
//import hasim.json.JsonJob;
//
//import java.io.*;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.InvocationTargetException;
//import java.net.*;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//
///** A Reduce task. */
public class HReduceTask_old  {
//	/**
//	 * Logger for this class
//	 */
//	private static final Logger LOG = Logger.getLogger(HReduceTask.class);
//
//  
//  
//  private int numMaps;
//  private HReduceCopier reduceCopier;
//
//  private HCompressionCodec codec;
//
//  HProgress progress;
//  
////  { 
////    progress.setStatus("reduce"); 
////    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
////  }
//
//  private HProgress copyPhase, sortPhase ,reducePhase;
//
//  SortedSet<Datum> mapOutputFilesOnDisk;
//
//  public HReduceTask_old( int numMaps) {
//    this.numMaps = numMaps;
//  }
//
//  public int getNumMaps() { return numMaps; }
//  
//    
//  // Get the input files for the reducer.
//  private List<Datum> getMapFiles( boolean isLocal) 
//  throws IOException {
//    List<Datum> fileList = new ArrayList<Datum>();
//    
//    return fileList;
//  }
//
//  
//
//  @SuppressWarnings("unchecked")
//  public void run() throws IOException {
//
//    // check if it is a cleanupJobTask
//    // Initialize the codec
//    
//      reduceCopier = new HReduceCopier();
//      reduceCopier.fetchOutputs();
//
//      copyPhase.setStatus("complete");                         // copy is already complete
//      //setPhase("SORT");
//
//   
//    // free up the data structures
//    mapOutputFilesOnDisk.clear();
//    
//    sortPhase.setStatus("complete");         // sort is complete
//    //setPhase(TaskStatus.Phase.REDUCE); 
//
//    //runNewReducer, runOldReducer
//    
//    //done(umbilical, reporter);
//  }
//
//
//
//
//  class HReduceCopier {
//	  
//
//    /** Reference to the task object */
//    
//    /** Number of ms before timing out a copy */
//    private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;
//    
//    /** Max events to fetch in one go from the tasktracker */
//    private static final int MAX_EVENTS_TO_FETCH = 10000;
//
//    /**
//     * our reduce task instance
//     */
//    private HReduceTask reduceTask;
//    
//    /**
//     * the list of map outputs currently being copied
//     */
//    private List<Datum> scheduledCopies;//map outputs locations
//    
//    /**
//     *  the results of dispatched copy attempts
//     */
//    private List<Datum> copyResults;
//    
//    /**
//     *  the number of outputs to copy in parallel
//     */
//    private int numCopiers;
//    
//    /**
//     *  a number that is set to the max #fetches we'd schedule and then
//     *  pause the schduling
//     */
//    private int maxInFlight;
//    
//    /**
//     * the amount of time spent on fetching one map output before considering 
//     * it as failed and notifying the jobtracker about it.
//     */
//    private int maxBackoff;
//    
//    /**
//     * busy hosts from which copies are being backed off
//     * Map of host -> next contact time
//     */
//    private Map<String, Long> penaltyBox;
//    
//    /**
//     * the set of unique hosts from which we are copying
//     */
//    private Set<String> uniqueHosts;
//    
//    /**
//     * A reference to the RamManager for writing the map outputs to.
//     */
//    
//    private HShuffleRamManager ramManager;
//    
//    
//    /**
//     * Number of files to merge at a time
//     */
//    private int ioSortFactor;
//    
//   
//    
//    /** 
//     * A flag to indicate when to exit localFS merge
//     */
//    private  boolean exitLocalFSMerge = false;
//
//    /** 
//     * A flag to indicate when to exit getMapEvents thread 
//     */
//    private boolean exitGetMapEvents = false;
//    
//    /**
//     * When we accumulate maxInMemOutputs number of files in ram, we merge/spill
//     */
//    private final int maxInMemOutputs;
//
//    /**
//     * Usage threshold for in-memory output accumulation.
//     */
//    private final float maxInMemCopyPer;
//
//    /**
//     * Maximum memory usage of map outputs to merge from memory into
//     * the reduce, in bytes.
//     */
//    private final long maxInMemReduce;
//
//    /**
//     * The threads for fetching the files.
//     */
//    private List<MapOutputCopier> copiers = null;
//    
////    /**
////     * The object for metrics reporting.
////     */
////    private ShuffleClientMetrics shuffleClientMetrics = null;
//    
//    /**
//     * the minimum interval between tasktracker polls
//     */
//    private static final long MIN_POLL_INTERVAL = 1000;
//    
//    /**
//     * a list of map output locations for fetch retrials 
//     */
//    private List<Datum> retryFetches =
//      new ArrayList<Datum>();
//    
//    /** 
//     * The set of required map outputs
//     */
//    private Set <Datum> copiedMapOutputs = 
//      Collections.synchronizedSet(new TreeSet<Datum>());
//    
//   
//    
//    private Random random = null;
//
//    /**
//     * the max of all the map completion times
//     */
//    private int maxMapRuntime;
//    
//    /**
//     * Maximum number of fetch-retries per-map.
//     */
//    private volatile int maxFetchRetriesPerMap;
//    
//    /**
//     * Combiner runner, if a combiner is needed
//     */
//    private HCombinerRunner combinerRunner;
//
//
//
//
//    
//
//    /** 
//     * List of in-memory map-outputs.
//     */
//    private final List<Datum> mapOutputsFilesInMemory =
//      Collections.synchronizedList(new LinkedList<Datum>());
//    
//    /**
//     * The map for (Hosts, List of MapIds from this Host) maintaining
//     * map output locations
//     */
//    private final Map<String, List<Datum>> mapLocations = 
//      new ConcurrentHashMap<String, List<Datum>>();
//    
//    
//    
//    
//    private int nextMapOutputCopierId = 0;
//    
//    /**
//     * Abstraction to track a map-output.
//     */
//    List<Datum> mapOutputLocation;
//    
//    
//   
//    
//    class HShuffleRamManager {
//      /* Maximum percentage of the in-memory limit that a single shuffle can 
//       * consume*/ 
//      private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;
//      
//      /* Maximum percentage of shuffle-threads which can be stalled 
//       * simultaneously after which a merge is triggered. */ 
//      private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;
//      
//      private final double maxSize;
//      private final double maxSingleShuffleLimit;
//      
//      private int size = 0;
//      
//      private Object dataAvailable = new Object();
//      private double fullSize = 0;
//      private int numPendingRequests = 0;
//      private int numRequiredMapOutputs = 0;
//      private int numClosed = 0;
//      private boolean closed = false;
//      
//      public HShuffleRamManager(JsonJob conf) throws IOException {
//        final double maxInMemCopyUse =
//          conf.getMapredJobShuffleInputBufferPercent();
//        if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
//          throw new IOException("mapred.job.shuffle.input.buffer.percent" +
//                                maxInMemCopyUse);
//        }
//        maxSize = conf.getMapredChildJavaOpts()* maxInMemCopyUse;
//        
//        maxSingleShuffleLimit = maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION;
//        LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize + 
//                 ", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
//      }
//      
//      public synchronized boolean reserve(Datum d) throws Exception {
//    	  double requestedSize= d.size;
//    	   InputStream in;
//    	  
//        // Wait till the request can be fulfilled...
//        while ((size + requestedSize) > maxSize) {
//          
//         
//
//          // Track pending requests
//          synchronized (dataAvailable) {
//            ++numPendingRequests;
//            dataAvailable.notify();
//          }
//
//          // Wait for memory to free up
//          wait();
//          
//          // Track pending requests
//          synchronized (dataAvailable) {
//            --numPendingRequests;
//          }
//        }
//        
//        size += requestedSize;
//        return true;
//      }
//      
//      public synchronized void unreserve(int requestedSize) {
//        size -= requestedSize;
//        
//        synchronized (dataAvailable) {
//          fullSize -= requestedSize;
//          --numClosed;
//        }
//        
//        // Notify the threads blocked on RamManager.reserve
//        notifyAll();
//      }
//      
//      public boolean waitForDataToMerge() throws InterruptedException {
//        boolean done = false;
//        synchronized (dataAvailable) {
//                 // Start in-memory merge if manager has been closed or...
//          while (!closed
//                 &&
//                 // In-memory threshold exceeded and at least two segments
//                 // have been fetched
//                 (getPercentUsed() < maxInMemCopyPer || numClosed < 2)
//                 &&
//                 // More than "mapred.inmem.merge.threshold" map outputs
//                 // have been fetched into memory
//                 (maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)
//                 && 
//                 // More than MAX... threads are blocked on the RamManager
//                 // or the blocked threads are the last map outputs to be
//                 // fetched. If numRequiredMapOutputs is zero, either
//                 // setNumCopiedMapOutputs has not been called (no map ouputs
//                 // have been fetched, so there is nothing to merge) or the
//                 // last map outputs being transferred without
//                 // contention, so a merge would be premature.
//                 (numPendingRequests < 
//                      numCopiers*MAX_STALLED_SHUFFLE_THREADS_FRACTION && 
//                  (0 == numRequiredMapOutputs ||
//                   numPendingRequests < numRequiredMapOutputs))) {
//            dataAvailable.wait();
//          }
//          done = closed;
//        }
//        return done;
//      }
//      
//      public void closeInMemoryFile(Datum requestedSize) {
//        synchronized (dataAvailable) {
//          fullSize += requestedSize.size;
//          ++numClosed;
//          dataAvailable.notify();
//        }
//      }
//      
//      public void setNumCopiedMapOutputs(int numRequiredMapOutputs) {
//        synchronized (dataAvailable) {
//          this.numRequiredMapOutputs = numRequiredMapOutputs;
//          dataAvailable.notify();
//        }
//      }
//      
//      public void close() {
//        synchronized (dataAvailable) {
//          closed = true;
//          LOG.info("Closed ram manager");
//          dataAvailable.notify();
//        }
//      }
//      
//      private double getPercentUsed() {
//        return fullSize/maxSize;
//      }
//
//      double getMemoryLimit() {
//        return maxSize;
//      }
//      
//      boolean canFitInMemory(Datum requestedSize) {
//        return (requestedSize.size < Integer.MAX_VALUE && 
//                requestedSize.size < maxSingleShuffleLimit);
//      }
//    }
//
//    /** Copies map outputs as they become available */
//    private class MapOutputCopier extends Thread {
//
//      private Datum currentLocation = null;
//      private int id = nextMapOutputCopierId++;
//      
//      // Decompression of map-outputs
////      private CompressionCodec codec = null;
////      private Decompressor decompressor = null;
////      
//      
//      
//      /**
//       * Get the current map output location.
//       */
//      public synchronized Datum getLocation() {
//        return currentLocation;
//      }
//      
//      private synchronized void start(Datum loc) {
//        currentLocation = loc;
//      }
//      
//      private synchronized void finish(Datum loc) {
//        if (currentLocation != null) {
//          LOG.debug(getName() + " finishing " + currentLocation + " =" + loc.size);
//          synchronized (copyResults) {
//            copyResults.add(new Datum(currentLocation));
//            copyResults.notify();
//          }
//          currentLocation = null;
//        }
//      }
//      
//      /** Loop forever and fetch map outputs as they become available.
//       * The thread exits when it is interrupted by {@link ReduceTaskRunner}
//       */
//      @Override
//      public void run() {
//        while (true) {        
//            Datum loc = null;
//            double size = -1;
//            
//            synchronized (scheduledCopies) {
//              while (scheduledCopies.isEmpty()) {
//                scheduledCopies.wait();
//              }
//              loc = scheduledCopies.remove(0);
//            }
//            
//              start(loc);
//              size = loc.size;
//            
//              finish(loc);
//            
//         
//        //TODO if (decompressor != null) odecPool.returnDecompressor(decompressor);
//          
//      
//     
// 
//    
//        // Copy the map output to a temp file whose name is unique to this attempt 
//        Datum tmpMapOutput = new Datum(loc.getName()+"-"+id, loc);
//        
//        // Copy the map output
//        MapOutput mapOutput = getMapOutput(loc, tmpMapOutput,
//                                           reduceId.getTaskID().getId());
//        if (mapOutput == null) {
//          throw new IOException("Failed to fetch map-output for " + 
//                                loc.getTaskAttemptId() + " from " + 
//                                loc.getHost());
//        }
//        
//        // The size of the map-output
//        long bytes = mapOutput.compressedSize;
//        
//        // lock the ReduceTask while we do the rename
//        synchronized (ReduceTask.this) {
//          if (copiedMapOutputs.contains(loc.getTaskId())) {
//            mapOutput.discard();
//            return CopyResult.OBSOLETE;
//          }
//
//          // Special case: discard empty map-outputs
//          if (bytes == 0) {
//            try {
//              mapOutput.discard();
//            } catch (IOException ioe) {
//              LOG.info("Couldn't discard output of " + loc.getTaskId());
//            }
//            
//            // Note that we successfully copied the map-output
//            noteCopiedMapOutput(loc.getTaskId());
//            
//            return bytes;
//          }
//          
//          // Process map-output
//          if (mapOutput.inMemory) {
//            // Save it in the synchronized list of map-outputs
//            mapOutputsFilesInMemory.add(mapOutput);
//          } else {
//            // Rename the temporary file to the final file; 
//            // ensure it is on the same partition
//            tmpMapOutput = mapOutput.file;
//            filename = new Path(tmpMapOutput.getParent(), filename.getName());
//            if (!localFileSys.rename(tmpMapOutput, filename)) {
//              localFileSys.delete(tmpMapOutput, true);
//              bytes = -1;
//              throw new IOException("Failed to rename map output " + 
//                  tmpMapOutput + " to " + filename);
//            }
//
//            synchronized (mapOutputFilesOnDisk) {        
//              addToMapOutputFilesOnDisk(localFileSys.getFileStatus(filename));
//            }
//          }
//
//          // Note that we successfully copied the map-output
//          noteCopiedMapOutput(loc.getTaskId());
//        }
//        
//        return bytes;
//      }
//      
//      /**
//       * Save the map taskid whose output we just copied.
//       * This function assumes that it has been synchronized on ReduceTask.this.
//       * 
//       * @param taskId map taskid
//       */
//      private void noteCopiedMapOutput(TaskID taskId) {
//        copiedMapOutputs.add(taskId);
//        ramManager.setNumCopiedMapOutputs(numMaps - copiedMapOutputs.size());
//      }
//
//      /**
//       * Get the map output into a local file (either in the inmemory fs or on the 
//       * local fs) from the remote server.
//       * We use the file system so that we generate checksum files on the data.
//       * @param mapOutputLoc map-output to be fetched
//       * @param filename the filename to write the data into
//       * @param connectionTimeout number of milliseconds for connection timeout
//       * @param readTimeout number of milliseconds for read timeout
//       * @return the path of the file that got created
//       * @throws IOException when something goes wrong
//       */
//      private boolean getMapOutput(Datum mapOutputLoc, HMapper mapId){
//        // Connect
//        
//        LOG.info("header: " + mapId.getName() + ", compressed len: " + mapOutputLoc.size +
//                 ", decompressed len: " + mapOutputLoc.size);
//
//        //We will put a file in memory if it meets certain criteria:
//        //1. The size of the (decompressed) file should be less than 25% of 
//        //    the total inmem fs
//        //2. There is space available in the inmem fs
//        
//        // Check if this map-output can be saved in-memory
//        boolean shuffleInMemory = ramManager.canFitInMemory(mapOutputLoc); 
//
//        // Shuffle
//        Datum mapOutput = null;
//        if (shuffleInMemory) { 
//          LOG.info("Shuffling " + mapOutputLoc.size + " bytes (" + 
//              mapOutputLoc.size + " raw bytes) " + 
//              "into RAM from " + mapOutputLoc.getName());
//
//          mapOutput = shuffleInMemory(mapOutputLoc, connection, input,
//                                      (int)decompressedLength,
//                                      (int)compressedLength);
//        } else {
//          LOG.info("Shuffling " + decompressedLength + " bytes (" + 
//              compressedLength + " raw bytes) " + 
//              "into Local-FS from " + mapOutputLoc.getTaskAttemptId());
//
//          mapOutput = shuffleToDisk(mapOutputLoc, input, filename, 
//              compressedLength);
//        }
//            
//        return mapOutput;
//      }
//
//      /** 
//       * The connection establishment is attempted multiple times and is given up 
//       * only on the last failure. Instead of connecting with a timeout of 
//       * X, we try connecting with a timeout of x < X but multiple times. 
//       */
//      private InputStream getInputStream(URLConnection connection, 
//                                         int connectionTimeout, 
//                                         int readTimeout) 
//      throws IOException {
//        int unit = 0;
//        if (connectionTimeout < 0) {
//          throw new IOException("Invalid timeout "
//                                + "[timeout = " + connectionTimeout + " ms]");
//        } else if (connectionTimeout > 0) {
//          unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout)
//                 ? connectionTimeout
//                 : UNIT_CONNECT_TIMEOUT;
//        }
//        // set the read timeout to the total timeout
//        connection.setReadTimeout(readTimeout);
//        // set the connect timeout to the unit-connect-timeout
//        connection.setConnectTimeout(unit);
//        while (true) {
//          try {
//            return connection.getInputStream();
//          } catch (IOException ioe) {
//            // update the total remaining connect-timeout
//            connectionTimeout -= unit;
//
//            // throw an exception if we have waited for timeout amount of time
//            // note that the updated value if timeout is used here
//            if (connectionTimeout == 0) {
//              throw ioe;
//            }
//
//            // reset the connect timeout for the last try
//            if (connectionTimeout < unit) {
//              unit = connectionTimeout;
//              // reset the connect time out for the final connect
//              connection.setConnectTimeout(unit);
//            }
//          }
//        }
//      }
//
//      private Datum shuffleInMemory(Datum mapOutputLoc){
//        // Reserve ram for the map-output
//        boolean createdNow = ramManager.reserve(mapOutputLoc);
//      
//        
//
//      
//        // Are map-outputs compressed?
//      
//        // Copy map-output into an in-memory buffer
//        Datum mapOutput = 
//          new Datum("in_mem_"+ mapOutputLoc.getName(), mapOutputLoc);
//        
//      
//        //gridsim netsend Datum 
//
//          LOG.info("Read " + mapOutput.size + " bytes from map-output for " +
//                   );
//
//
//        // Close the in-memory file
//        ramManager.closeInMemoryFile(mapOutputLoc);
//
//        
//        
//        return mapOutput;
//      }
//      
//      private MapOutput shuffleToDisk(MapOutputLocation mapOutputLoc,
//                                      InputStream input,
//                                      Path filename,
//                                      long mapOutputLength) 
//      throws IOException {
//        // Find out a suitable location for the output on local-filesystem
//        Path localFilename = 
//          lDirAlloc.getLocalPathForWrite(filename.toUri().getPath(), 
//                                         mapOutputLength, conf);
//
//        MapOutput mapOutput = 
//          new MapOutput(mapOutputLoc.getTaskId(), mapOutputLoc.getTaskAttemptId(), 
//                        conf, localFileSys.makeQualified(localFilename), 
//                        mapOutputLength);
//
//
//        // Copy data to local-disk
//        OutputStream output = null;
//        long bytesRead = 0;
//        try {
//          output = rfs.create(localFilename);
//          
//          byte[] buf = new byte[64 * 1024];
//          int n = input.read(buf, 0, buf.length);
//          while (n > 0) {
//            bytesRead += n;
//            shuffleClientMetrics.inputBytes(n);
//            output.write(buf, 0, n);
//
//            // indicate we're making progress
//            reporter.progress();
//            n = input.read(buf, 0, buf.length);
//          }
//
//          LOG.info("Read " + bytesRead + " bytes from map-output for " +
//              mapOutputLoc.getTaskAttemptId());
//
//          output.close();
//          input.close();
//        } catch (IOException ioe) {
//          LOG.info("Failed to shuffle from " + mapOutputLoc.getTaskAttemptId(), 
//                   ioe);
//
//          // Discard the map-output
//          try {
//            mapOutput.discard();
//          } catch (IOException ignored) {
//            LOG.info("Failed to discard map-output from " + 
//                mapOutputLoc.getTaskAttemptId(), ignored);
//          }
//          mapOutput = null;
//
//          // Close the streams
//          IOUtils.cleanup(LOG, input, output);
//
//          // Re-throw
//          throw ioe;
//        }
//
//        // Sanity check
//        if (bytesRead != mapOutputLength) {
//          try {
//            mapOutput.discard();
//          } catch (Exception ioe) {
//            // IGNORED because we are cleaning up
//            LOG.info("Failed to discard map-output from " + 
//                mapOutputLoc.getTaskAttemptId(), ioe);
//          } catch (Throwable t) {
//            String msg = getTaskID() + " : Failed in shuffle to disk :" 
//                         + StringUtils.stringifyException(t);
//            reportFatalError(getTaskID(), t, msg);
//          }
//          mapOutput = null;
//
//          throw new IOException("Incomplete map output received for " +
//                                mapOutputLoc.getTaskAttemptId() + " from " +
//                                mapOutputLoc.getOutputLocation() + " (" + 
//                                bytesRead + " instead of " + 
//                                mapOutputLength + ")"
//          );
//        }
//
//        return mapOutput;
//
//      }
//      
//    } // MapOutputCopier
//    
//    private void configureClasspath(JobConf conf)
//      throws IOException {
//      
//      // get the task and the current classloader which will become the parent
//      Task task = ReduceTask.this;
//      ClassLoader parent = conf.getClassLoader();   
//      
//      // get the work directory which holds the elements we are dynamically
//      // adding to the classpath
//      File workDir = new File(task.getJobFile()).getParentFile();
//      ArrayList<URL> urllist = new ArrayList<URL>();
//      
//      // add the jars and directories to the classpath
//      String jar = conf.getJar();
//      if (jar != null) {      
//        File jobCacheDir = new File(new Path(jar).getParent().toString());
//
//        File[] libs = new File(jobCacheDir, "lib").listFiles();
//        if (libs != null) {
//          for (int i = 0; i < libs.length; i++) {
//            urllist.add(libs[i].toURL());
//          }
//        }
//        urllist.add(new File(jobCacheDir, "classes").toURL());
//        urllist.add(jobCacheDir.toURL());
//        
//      }
//      urllist.add(workDir.toURL());
//      
//      // create a new classloader with the old classloader as its parent
//      // then set that classloader as the one used by the current jobconf
//      URL[] urls = urllist.toArray(new URL[urllist.size()]);
//      URLClassLoader loader = new URLClassLoader(urls, parent);
//      conf.setClassLoader(loader);
//    }
//    
//    public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf,
//                        TaskReporter reporter
//                        )throws ClassNotFoundException, IOException {
//      
//      configureClasspath(conf);
//      this.reporter = reporter;
//      this.shuffleClientMetrics = new ShuffleClientMetrics(conf);
//      this.umbilical = umbilical;      
//      this.reduceTask = ReduceTask.this;
//
//      this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
//      this.copyResults = new ArrayList<CopyResult>(100);    
//      this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
//      this.maxInFlight = 4 * numCopiers;
//      this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);
//      Counters.Counter combineInputCounter = 
//        reporter.getCounter(Task.Counter.COMBINE_INPUT_RECORDS);
//      this.combinerRunner = CombinerRunner.create(conf, getTaskID(),
//                                                  combineInputCounter,
//                                                  reporter, null);
//      if (combinerRunner != null) {
//        combineCollector = 
//          new CombineOutputCollector(reduceCombineOutputCounter);
//      }
//      
//      this.ioSortFactor = conf.getInt("io.sort.factor", 10);
//      // the exponential backoff formula
//      //    backoff (t) = init * base^(t-1)
//      // so for max retries we get
//      //    backoff(1) + .... + backoff(max_fetch_retries) ~ max
//      // solving which we get
//      //    max_fetch_retries ~ log((max * (base - 1) / init) + 1) / log(base)
//      // for the default value of max = 300 (5min) we get max_fetch_retries = 6
//      // the order is 4,8,16,32,64,128. sum of which is 252 sec = 4.2 min
//      
//      // optimizing for the base 2
//      this.maxFetchRetriesPerMap = Math.max(MIN_FETCH_RETRIES_PER_MAP, 
//             getClosestPowerOf2((this.maxBackoff * 1000 / BACKOFF_INIT) + 1));
//      this.maxFailedUniqueFetches = Math.min(numMaps, 
//                                             this.maxFailedUniqueFetches);
//      this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold", 1000);
//      this.maxInMemCopyPer =
//        conf.getFloat("mapred.job.shuffle.merge.percent", 0.66f);
//      final float maxRedPer =
//        conf.getFloat("mapred.job.reduce.input.buffer.percent", 0f);
//      if (maxRedPer > 1.0 || maxRedPer < 0.0) {
//        throw new IOException("mapred.job.reduce.input.buffer.percent" +
//                              maxRedPer);
//      }
//      this.maxInMemReduce = (int)Math.min(
//          Runtime.getRuntime().maxMemory() * maxRedPer, Integer.MAX_VALUE);
//
//      // Setup the RamManager
//      ramManager = new ShuffleRamManager(conf);
//
//      localFileSys = FileSystem.getLocal(conf);
//
//      rfs = ((LocalFileSystem)localFileSys).getRaw();
//
//      // hosts -> next contact time
//      this.penaltyBox = new LinkedHashMap<String, Long>();
//      
//      // hostnames
//      this.uniqueHosts = new HashSet<String>();
//      
//      // Seed the random number generator with a reasonably globally unique seed
//      long randomSeed = System.nanoTime() + 
//                        (long)Math.pow(this.reduceTask.getPartition(),
//                                       (this.reduceTask.getPartition()%10)
//                                      );
//      this.random = new Random(randomSeed);
//      this.maxMapRuntime = 0;
//    }
//    
//    private boolean busyEnough(int numInFlight) {
//      return numInFlight > maxInFlight;
//    }
//    
//    
//    public boolean fetchOutputs() throws IOException {
//      int totalFailures = 0;
//      int            numInFlight = 0, numCopied = 0;
//      DecimalFormat  mbpsFormat = new DecimalFormat("0.00");
//      final Progress copyPhase = 
//        reduceTask.getProgress().phase();
//      LocalFSMerger localFSMergerThread = null;
//      InMemFSMergeThread inMemFSMergeThread = null;
//      GetMapEventsThread getMapEventsThread = null;
//      
//      for (int i = 0; i < numMaps; i++) {
//        copyPhase.addPhase();       // add sub-phase per file
//      }
//      
//      copiers = new ArrayList<MapOutputCopier>(numCopiers);
//      
//      // start all the copying threads
//      for (int i=0; i < numCopiers; i++) {
//        MapOutputCopier copier = new MapOutputCopier(conf, reporter);
//        copiers.add(copier);
//        copier.start();
//      }
//      
//      //start the on-disk-merge thread
//      localFSMergerThread = new LocalFSMerger((LocalFileSystem)localFileSys);
//      //start the in memory merger thread
//      inMemFSMergeThread = new InMemFSMergeThread();
//      localFSMergerThread.start();
//      inMemFSMergeThread.start();
//      
//      // start the map events thread
//      getMapEventsThread = new GetMapEventsThread();
//      getMapEventsThread.start();
//      
//      // start the clock for bandwidth measurement
//      long startTime = System.currentTimeMillis();
//      long currentTime = startTime;
//      long lastProgressTime = startTime;
//      long lastOutputTime = 0;
//      
//        // loop until we get all required outputs
//        while (copiedMapOutputs.size() < numMaps && mergeThrowable == null) {
//          
//          currentTime = System.currentTimeMillis();
//          boolean logNow = false;
//          if (currentTime - lastOutputTime > MIN_LOG_TIME) {
//            lastOutputTime = currentTime;
//            logNow = true;
//          }
//          if (logNow) {
//            LOG.info(reduceTask.getTaskID() + " Need another " 
//                   + (numMaps - copiedMapOutputs.size()) + " map output(s) "
//                   + "where " + numInFlight + " is already in progress");
//          }
//
//          // Put the hash entries for the failed fetches.
//          Iterator<MapOutputLocation> locItr = retryFetches.iterator();
//
//          while (locItr.hasNext()) {
//            MapOutputLocation loc = locItr.next(); 
//            List<MapOutputLocation> locList = 
//              mapLocations.get(loc.getHost());
//            
//            // Check if the list exists. Map output location mapping is cleared 
//            // once the jobtracker restarts and is rebuilt from scratch.
//            // Note that map-output-location mapping will be recreated and hence
//            // we continue with the hope that we might find some locations
//            // from the rebuild map.
//            if (locList != null) {
//              // Add to the beginning of the list so that this map is 
//              //tried again before the others and we can hasten the 
//              //re-execution of this map should there be a problem
//              locList.add(0, loc);
//            }
//          }
//
//          if (retryFetches.size() > 0) {
//            LOG.info(reduceTask.getTaskID() + ": " +  
//                  "Got " + retryFetches.size() +
//                  " map-outputs from previous failures");
//          }
//          // clear the "failed" fetches hashmap
//          retryFetches.clear();
//
//          // now walk through the cache and schedule what we can
//          int numScheduled = 0;
//          int numDups = 0;
//          
//          synchronized (scheduledCopies) {
//  
//            // Randomize the map output locations to prevent 
//            // all reduce-tasks swamping the same tasktracker
//            List<String> hostList = new ArrayList<String>();
//            hostList.addAll(mapLocations.keySet()); 
//            
//            Collections.shuffle(hostList, this.random);
//              
//            Iterator<String> hostsItr = hostList.iterator();
//
//            while (hostsItr.hasNext()) {
//            
//              String host = hostsItr.next();
//
//              List<MapOutputLocation> knownOutputsByLoc = 
//                mapLocations.get(host);
//
//              // Check if the list exists. Map output location mapping is 
//              // cleared once the jobtracker restarts and is rebuilt from 
//              // scratch.
//              // Note that map-output-location mapping will be recreated and 
//              // hence we continue with the hope that we might find some 
//              // locations from the rebuild map and add then for fetching.
//              if (knownOutputsByLoc == null || knownOutputsByLoc.size() == 0) {
//                continue;
//              }
//              
//              //Identify duplicate hosts here
//              if (uniqueHosts.contains(host)) {
//                 numDups += knownOutputsByLoc.size(); 
//                 continue;
//              }
//
//              Long penaltyEnd = penaltyBox.get(host);
//              boolean penalized = false;
//            
//              if (penaltyEnd != null) {
//                if (currentTime < penaltyEnd.longValue()) {
//                  penalized = true;
//                } else {
//                  penaltyBox.remove(host);
//                }
//              }
//              
//              if (penalized)
//                continue;
//
//              synchronized (knownOutputsByLoc) {
//              
//                locItr = knownOutputsByLoc.iterator();
//            
//                while (locItr.hasNext()) {
//              
//                  MapOutputLocation loc = locItr.next();
//              
//                  // Do not schedule fetches from OBSOLETE maps
//                  if (obsoleteMapIds.contains(loc.getTaskAttemptId())) {
//                    locItr.remove();
//                    continue;
//                  }
//
//                  uniqueHosts.add(host);
//                  scheduledCopies.add(loc);
//                  locItr.remove();  // remove from knownOutputs
//                  numInFlight++; numScheduled++;
//
//                  break; //we have a map from this host
//                }
//              }
//            }
//            scheduledCopies.notifyAll();
//          }
//
//          if (numScheduled > 0 || logNow) {
//            LOG.info(reduceTask.getTaskID() + " Scheduled " + numScheduled +
//                   " outputs (" + penaltyBox.size() +
//                   " slow hosts and" + numDups + " dup hosts)");
//          }
//
//          if (penaltyBox.size() > 0 && logNow) {
//            LOG.info("Penalized(slow) Hosts: ");
//            for (String host : penaltyBox.keySet()) {
//              LOG.info(host + " Will be considered after: " + 
//                  ((penaltyBox.get(host) - currentTime)/1000) + " seconds.");
//            }
//          }
//
//          // if we have no copies in flight and we can't schedule anything
//          // new, just wait for a bit
//          try {
//            if (numInFlight == 0 && numScheduled == 0) {
//              // we should indicate progress as we don't want TT to think
//              // we're stuck and kill us
//              reporter.progress();
//              Thread.sleep(5000);
//            }
//          } catch (InterruptedException e) { } // IGNORE
//          
//          while (numInFlight > 0 && mergeThrowable == null) {
//            LOG.debug(reduceTask.getTaskID() + " numInFlight = " + 
//                      numInFlight);
//            //the call to getCopyResult will either 
//            //1) return immediately with a null or a valid CopyResult object,
//            //                 or
//            //2) if the numInFlight is above maxInFlight, return with a 
//            //   CopyResult object after getting a notification from a 
//            //   fetcher thread, 
//            //So, when getCopyResult returns null, we can be sure that
//            //we aren't busy enough and we should go and get more mapcompletion
//            //events from the tasktracker
//            CopyResult cr = getCopyResult(numInFlight);
//
//            if (cr == null) {
//              break;
//            }
//            
//            if (cr.getSuccess()) {  // a successful copy
//              numCopied++;
//              lastProgressTime = System.currentTimeMillis();
//              reduceShuffleBytes.increment(cr.getSize());
//                
//              long secsSinceStart = 
//                (System.currentTimeMillis()-startTime)/1000+1;
//              float mbs = ((float)reduceShuffleBytes.getCounter())/(1024*1024);
//              float transferRate = mbs/secsSinceStart;
//                
//              copyPhase.startNextPhase();
//              copyPhase.setStatus("copy (" + numCopied + " of " + numMaps 
//                                  + " at " +
//                                  mbpsFormat.format(transferRate) +  " MB/s)");
//                
//              // Note successful fetch for this mapId to invalidate
//              // (possibly) old fetch-failures
//              fetchFailedMaps.remove(cr.getLocation().getTaskId());
//            } else if (cr.isObsolete()) {
//              //ignore
//              LOG.info(reduceTask.getTaskID() + 
//                       " Ignoring obsolete copy result for Map Task: " + 
//                       cr.getLocation().getTaskAttemptId() + " from host: " + 
//                       cr.getHost());
//            } else {
//              retryFetches.add(cr.getLocation());
//              
//              // note the failed-fetch
//              TaskAttemptID mapTaskId = cr.getLocation().getTaskAttemptId();
//              TaskID mapId = cr.getLocation().getTaskId();
//              
//              totalFailures++;
//              Integer noFailedFetches = 
//                mapTaskToFailedFetchesMap.get(mapTaskId);
//              noFailedFetches = 
//                (noFailedFetches == null) ? 1 : (noFailedFetches + 1);
//              mapTaskToFailedFetchesMap.put(mapTaskId, noFailedFetches);
//              LOG.info("Task " + getTaskID() + ": Failed fetch #" + 
//                       noFailedFetches + " from " + mapTaskId);
//              
//              // did the fetch fail too many times?
//              // using a hybrid technique for notifying the jobtracker.
//              //   a. the first notification is sent after max-retries 
//              //   b. subsequent notifications are sent after 2 retries.   
//              if ((noFailedFetches >= maxFetchRetriesPerMap) 
//                  && ((noFailedFetches - maxFetchRetriesPerMap) % 2) == 0) {
//                synchronized (ReduceTask.this) {
//                  taskStatus.addFetchFailedMap(mapTaskId);
//                  LOG.info("Failed to fetch map-output from " + mapTaskId + 
//                           " even after MAX_FETCH_RETRIES_PER_MAP retries... "
//                           + " reporting to the JobTracker");
//                }
//              }
//              // note unique failed-fetch maps
//              if (noFailedFetches == maxFetchRetriesPerMap) {
//                fetchFailedMaps.add(mapId);
//                  
//                // did we have too many unique failed-fetch maps?
//                // and did we fail on too many fetch attempts?
//                // and did we progress enough
//                //     or did we wait for too long without any progress?
//               
//                // check if the reducer is healthy
//                boolean reducerHealthy = 
//                    (((float)totalFailures / (totalFailures + numCopied)) 
//                     < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);
//                
//                // check if the reducer has progressed enough
//                boolean reducerProgressedEnough = 
//                    (((float)numCopied / numMaps) 
//                     >= MIN_REQUIRED_PROGRESS_PERCENT);
//                
//                // check if the reducer is stalled for a long time
//                // duration for which the reducer is stalled
//                int stallDuration = 
//                    (int)(System.currentTimeMillis() - lastProgressTime);
//                // duration for which the reducer ran with progress
//                int shuffleProgressDuration = 
//                    (int)(lastProgressTime - startTime);
//                // min time the reducer should run without getting killed
//                int minShuffleRunDuration = 
//                    (shuffleProgressDuration > maxMapRuntime) 
//                    ? shuffleProgressDuration 
//                    : maxMapRuntime;
//                boolean reducerStalled = 
//                    (((float)stallDuration / minShuffleRunDuration) 
//                     >= MAX_ALLOWED_STALL_TIME_PERCENT);
//                
//                // kill if not healthy and has insufficient progress
//                if ((fetchFailedMaps.size() >= maxFailedUniqueFetches ||
//                     fetchFailedMaps.size() == (numMaps - copiedMapOutputs.size()))
//                    && !reducerHealthy 
//                    && (!reducerProgressedEnough || reducerStalled)) { 
//                  LOG.fatal("Shuffle failed with too many fetch failures " + 
//                            "and insufficient progress!" +
//                            "Killing task " + getTaskID() + ".");
//                  umbilical.shuffleError(getTaskID(), 
//                                         "Exceeded MAX_FAILED_UNIQUE_FETCHES;"
//                                         + " bailing-out.");
//                }
//              }
//                
//              // back off exponentially until num_retries <= max_retries
//              // back off by max_backoff/2 on subsequent failed attempts
//              currentTime = System.currentTimeMillis();
//              int currentBackOff = noFailedFetches <= maxFetchRetriesPerMap 
//                                   ? BACKOFF_INIT 
//                                     * (1 << (noFailedFetches - 1)) 
//                                   : (this.maxBackoff * 1000 / 2);
//              penaltyBox.put(cr.getHost(), currentTime + currentBackOff);
//              LOG.warn(reduceTask.getTaskID() + " adding host " +
//                       cr.getHost() + " to penalty box, next contact in " +
//                       (currentBackOff/1000) + " seconds");
//            }
//            uniqueHosts.remove(cr.getHost());
//            numInFlight--;
//          }
//        }
//        
//        // all done, inform the copiers to exit
//        exitGetMapEvents= true;
//        try {
//          getMapEventsThread.join();
//          LOG.info("getMapsEventsThread joined.");
//        } catch (InterruptedException ie) {
//          LOG.info("getMapsEventsThread threw an exception: " +
//              StringUtils.stringifyException(ie));
//        }
//
//        synchronized (copiers) {
//          synchronized (scheduledCopies) {
//            for (MapOutputCopier copier : copiers) {
//              copier.interrupt();
//            }
//            copiers.clear();
//          }
//        }
//        
//        // copiers are done, exit and notify the waiting merge threads
//        synchronized (mapOutputFilesOnDisk) {
//          exitLocalFSMerge = true;
//          mapOutputFilesOnDisk.notify();
//        }
//        
//        ramManager.close();
//        
//        //Do a merge of in-memory files (if there are any)
//        if (mergeThrowable == null) {
//          try {
//            // Wait for the on-disk merge to complete
//            localFSMergerThread.join();
//            LOG.info("Interleaved on-disk merge complete: " + 
//                     mapOutputFilesOnDisk.size() + " files left.");
//            
//            //wait for an ongoing merge (if it is in flight) to complete
//            inMemFSMergeThread.join();
//            LOG.info("In-memory merge complete: " + 
//                     mapOutputsFilesInMemory.size() + " files left.");
//            } catch (InterruptedException ie) {
//            LOG.warn(reduceTask.getTaskID() +
//                     " Final merge of the inmemory files threw an exception: " + 
//                     StringUtils.stringifyException(ie));
//            // check if the last merge generated an error
//            if (mergeThrowable != null) {
//              mergeThrowable = ie;
//            }
//            return false;
//          }
//        }
//        return mergeThrowable == null && copiedMapOutputs.size() == numMaps;
//    }
//    
//    private long createInMemorySegments(
//        List<Segment<K, V>> inMemorySegments, long leaveBytes)
//        throws IOException {
//      long totalSize = 0L;
//      synchronized (mapOutputsFilesInMemory) {
//        // fullSize could come from the RamManager, but files can be
//        // closed but not yet present in mapOutputsFilesInMemory
//        long fullSize = 0L;
//        for (MapOutput mo : mapOutputsFilesInMemory) {
//          fullSize += mo.data.length;
//        }
//        while(fullSize > leaveBytes) {
//          MapOutput mo = mapOutputsFilesInMemory.remove(0);
//          totalSize += mo.data.length;
//          fullSize -= mo.data.length;
//          Reader<K, V> reader = 
//            new InMemoryReader<K, V>(ramManager, mo.mapAttemptId,
//                                     mo.data, 0, mo.data.length);
//          Segment<K, V> segment = 
//            new Segment<K, V>(reader, true);
//          inMemorySegments.add(segment);
//        }
//      }
//      return totalSize;
//    }
//
//    /**
//     * Create a RawKeyValueIterator from copied map outputs. All copying
//     * threads have exited, so all of the map outputs are available either in
//     * memory or on disk. We also know that no merges are in progress, so
//     * synchronization is more lax, here.
//     *
//     * The iterator returned must satisfy the following constraints:
//     *   1. Fewer than io.sort.factor files may be sources
//     *   2. No more than maxInMemReduce bytes of map outputs may be resident
//     *      in memory when the reduce begins
//     *
//     * If we must perform an intermediate merge to satisfy (1), then we can
//     * keep the excluded outputs from (2) in memory and include them in the
//     * first merge pass. If not, then said outputs must be written to disk
//     * first.
//     */
//    @SuppressWarnings("unchecked")
//    private RawKeyValueIterator createKVIterator(
//        JobConf job, FileSystem fs, Reporter reporter) throws IOException {
//
//      // merge config params
//      Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
//      Class<V> valueClass = (Class<V>)job.getMapOutputValueClass();
//      boolean keepInputs = job.getKeepFailedTaskFiles();
//      final Path tmpDir = new Path(getTaskID().toString());
//      final RawComparator<K> comparator =
//        (RawComparator<K>)job.getOutputKeyComparator();
//
//      // segments required to vacate memory
//      List<Segment<K,V>> memDiskSegments = new ArrayList<Segment<K,V>>();
//      long inMemToDiskBytes = 0;
//      if (mapOutputsFilesInMemory.size() > 0) {
//        TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
//        inMemToDiskBytes = createInMemorySegments(memDiskSegments,
//            maxInMemReduce);
//        final int numMemDiskSegments = memDiskSegments.size();
//        if (numMemDiskSegments > 0 &&
//              ioSortFactor > mapOutputFilesOnDisk.size()) {
//          // must spill to disk, but can't retain in-mem for intermediate merge
//          final Path outputPath = mapOutputFile.getInputFileForWrite(mapId,
//                            reduceTask.getTaskID(), inMemToDiskBytes);
//          final RawKeyValueIterator rIter = Merger.merge(job, fs,
//              keyClass, valueClass, memDiskSegments, numMemDiskSegments,
//              tmpDir, comparator, reporter, spilledRecordsCounter, null);
//          final Writer writer = new Writer(job, fs, outputPath,
//              keyClass, valueClass, codec, null);
//          try {
//            Merger.writeFile(rIter, writer, reporter, job);
//            addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath));
//          } catch (Exception e) {
//            if (null != outputPath) {
//              fs.delete(outputPath, true);
//            }
//            throw new IOException("Final merge failed", e);
//          } finally {
//            if (null != writer) {
//              writer.close();
//            }
//          }
//          LOG.info("Merged " + numMemDiskSegments + " segments, " +
//                   inMemToDiskBytes + " bytes to disk to satisfy " +
//                   "reduce memory limit");
//          inMemToDiskBytes = 0;
//          memDiskSegments.clear();
//        } else if (inMemToDiskBytes != 0) {
//          LOG.info("Keeping " + numMemDiskSegments + " segments, " +
//                   inMemToDiskBytes + " bytes in memory for " +
//                   "intermediate, on-disk merge");
//        }
//      }
//
//      // segments on disk
//      List<Segment<K,V>> diskSegments = new ArrayList<Segment<K,V>>();
//      long onDiskBytes = inMemToDiskBytes;
//      Path[] onDisk = getMapFiles(fs, false);
//      for (Path file : onDisk) {
//        onDiskBytes += fs.getFileStatus(file).getLen();
//        diskSegments.add(new Segment<K, V>(job, fs, file, codec, keepInputs));
//      }
//      LOG.info("Merging " + onDisk.length + " files, " +
//               onDiskBytes + " bytes from disk");
//      Collections.sort(diskSegments, new Comparator<Segment<K,V>>() {
//        public int compare(Segment<K, V> o1, Segment<K, V> o2) {
//          if (o1.getLength() == o2.getLength()) {
//            return 0;
//          }
//          return o1.getLength() < o2.getLength() ? -1 : 1;
//        }
//      });
//
//      // build final list of segments from merged backed by disk + in-mem
//      List<Segment<K,V>> finalSegments = new ArrayList<Segment<K,V>>();
//      long inMemBytes = createInMemorySegments(finalSegments, 0);
//      LOG.info("Merging " + finalSegments.size() + " segments, " +
//               inMemBytes + " bytes from memory into reduce");
//      if (0 != onDiskBytes) {
//        final int numInMemSegments = memDiskSegments.size();
//        diskSegments.addAll(0, memDiskSegments);
//        memDiskSegments.clear();
//        RawKeyValueIterator diskMerge = Merger.merge(
//            job, fs, keyClass, valueClass, codec, diskSegments,
//            ioSortFactor, numInMemSegments, tmpDir, comparator,
//            reporter, false, spilledRecordsCounter, null);
//        diskSegments.clear();
//        if (0 == finalSegments.size()) {
//          return diskMerge;
//        }
//        finalSegments.add(new Segment<K,V>(
//              new RawKVIteratorReader(diskMerge, onDiskBytes), true));
//      }
//      return Merger.merge(job, fs, keyClass, valueClass,
//                   finalSegments, finalSegments.size(), tmpDir,
//                   comparator, reporter, spilledRecordsCounter, null);
//    }
//
//    class RawKVIteratorReader extends IFile.Reader<K,V> {
//
//      private final RawKeyValueIterator kvIter;
//
//      public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
//          throws IOException {
//        super(null, null, size, null, spilledRecordsCounter);
//        this.kvIter = kvIter;
//      }
//
//      public boolean next(DataInputBuffer key, DataInputBuffer value)
//          throws IOException {
//        if (kvIter.next()) {
//          final DataInputBuffer kb = kvIter.getKey();
//          final DataInputBuffer vb = kvIter.getValue();
//          final int kp = kb.getPosition();
//          final int klen = kb.getLength() - kp;
//          key.reset(kb.getData(), kp, klen);
//          final int vp = vb.getPosition();
//          final int vlen = vb.getLength() - vp;
//          value.reset(vb.getData(), vp, vlen);
//          bytesRead += klen + vlen;
//          return true;
//        }
//        return false;
//      }
//
//      public long getPosition() throws IOException {
//        return bytesRead;
//      }
//
//      public void close() throws IOException {
//        kvIter.close();
//      }
//    }
//
//    private CopyResult getCopyResult(int numInFlight) {  
//      synchronized (copyResults) {
//        while (copyResults.isEmpty()) {
//          try {
//            //The idea is that if we have scheduled enough, we can wait until
//            //we hear from one of the copiers.
//            if (busyEnough(numInFlight)) {
//              copyResults.wait();
//            } else {
//              return null;
//            }
//          } catch (InterruptedException e) { }
//        }
//        return copyResults.remove(0);
//      }    
//    }
//    
//    private void addToMapOutputFilesOnDisk(FileStatus status) {
//      synchronized (mapOutputFilesOnDisk) {
//        mapOutputFilesOnDisk.add(status);
//        mapOutputFilesOnDisk.notify();
//      }
//    }
//    
//    
//    
//    /** Starts merging the local copy (on disk) of the map's output so that
//     * most of the reducer's input is sorted i.e overlapping shuffle
//     * and merge phases.
//     */
//    private class LocalFSMerger extends Thread {
//      private LocalFileSystem localFileSys;
//
//      public LocalFSMerger(LocalFileSystem fs) {
//        this.localFileSys = fs;
//        setName("Thread for merging on-disk files");
//        setDaemon(true);
//      }
//
//      @SuppressWarnings("unchecked")
//      public void run() {
//        try {
//          LOG.info(reduceTask.getTaskID() + " Thread started: " + getName());
//          while(!exitLocalFSMerge){
//            synchronized (mapOutputFilesOnDisk) {
//              while (!exitLocalFSMerge &&
//                  mapOutputFilesOnDisk.size() < (2 * ioSortFactor - 1)) {
//                LOG.info(reduceTask.getTaskID() + " Thread waiting: " + getName());
//                mapOutputFilesOnDisk.wait();
//              }
//            }
//            if(exitLocalFSMerge) {//to avoid running one extra time in the end
//              break;
//            }
//            List<Path> mapFiles = new ArrayList<Path>();
//            long approxOutputSize = 0;
//            int bytesPerSum = 
//              reduceTask.getConf().getInt("io.bytes.per.checksum", 512);
//            LOG.info(reduceTask.getTaskID() + "We have  " + 
//                mapOutputFilesOnDisk.size() + " map outputs on disk. " +
//                "Triggering merge of " + ioSortFactor + " files");
//            // 1. Prepare the list of files to be merged. This list is prepared
//            // using a list of map output files on disk. Currently we merge
//            // io.sort.factor files into 1.
//            synchronized (mapOutputFilesOnDisk) {
//              for (int i = 0; i < ioSortFactor; ++i) {
//                FileStatus filestatus = mapOutputFilesOnDisk.first();
//                mapOutputFilesOnDisk.remove(filestatus);
//                mapFiles.add(filestatus.getPath());
//                approxOutputSize += filestatus.getLen();
//              }
//            }
//            
//            // sanity check
//            if (mapFiles.size() == 0) {
//                return;
//            }
//            
//            // add the checksum length
//            approxOutputSize += ChecksumFileSystem
//                                .getChecksumLength(approxOutputSize,
//                                                   bytesPerSum);
//  
//            // 2. Start the on-disk merge process
//            Path outputPath = 
//              lDirAlloc.getLocalPathForWrite(mapFiles.get(0).toString(), 
//                                             approxOutputSize, conf)
//              .suffix(".merged");
//            Writer writer = 
//              new Writer(conf,rfs, outputPath, 
//                         conf.getMapOutputKeyClass(), 
//                         conf.getMapOutputValueClass(),
//                         codec, null);
//            RawKeyValueIterator iter  = null;
//            Path tmpDir = new Path(reduceTask.getTaskID().toString());
//            try {
//              iter = Merger.merge(conf, rfs,
//                                  conf.getMapOutputKeyClass(),
//                                  conf.getMapOutputValueClass(),
//                                  codec, mapFiles.toArray(new Path[mapFiles.size()]), 
//                                  true, ioSortFactor, tmpDir, 
//                                  conf.getOutputKeyComparator(), reporter,
//                                  spilledRecordsCounter, null);
//              
//              Merger.writeFile(iter, writer, reporter, conf);
//              writer.close();
//            } catch (Exception e) {
//              localFileSys.delete(outputPath, true);
//              throw new IOException (StringUtils.stringifyException(e));
//            }
//            
//            synchronized (mapOutputFilesOnDisk) {
//              addToMapOutputFilesOnDisk(localFileSys.getFileStatus(outputPath));
//            }
//            
//            LOG.info(reduceTask.getTaskID() +
//                     " Finished merging " + mapFiles.size() + 
//                     " map output files on disk of total-size " + 
//                     approxOutputSize + "." + 
//                     " Local output file is " + outputPath + " of size " +
//                     localFileSys.getFileStatus(outputPath).getLen());
//            }
//        } catch (Exception e) {
//          LOG.warn(reduceTask.getTaskID()
//                   + " Merging of the local FS files threw an exception: "
//                   + StringUtils.stringifyException(e));
//          if (mergeThrowable == null) {
//            mergeThrowable = e;
//          }
//        } catch (Throwable t) {
//          String msg = getTaskID() + " : Failed to merge on the local FS" 
//                       + StringUtils.stringifyException(t);
//          reportFatalError(getTaskID(), t, msg);
//        }
//      }
//    }
//
//    private class InMemFSMergeThread extends Thread {
//      
//      public InMemFSMergeThread() {
//        setName("Thread for merging in memory files");
//        setDaemon(true);
//      }
//      
//      public void run() {
//        LOG.info(reduceTask.getTaskID() + " Thread started: " + getName());
//        try {
//          boolean exit = false;
//          do {
//            exit = ramManager.waitForDataToMerge();
//            if (!exit) {
//              doInMemMerge();
//            }
//          } while (!exit);
//        } catch (Exception e) {
//          LOG.warn(reduceTask.getTaskID() +
//                   " Merge of the inmemory files threw an exception: "
//                   + StringUtils.stringifyException(e));
//          ReduceCopier.this.mergeThrowable = e;
//        } catch (Throwable t) {
//          String msg = getTaskID() + " : Failed to merge in memory" 
//                       + StringUtils.stringifyException(t);
//          reportFatalError(getTaskID(), t, msg);
//        }
//      }
//      
//      @SuppressWarnings("unchecked")
//      private void doInMemMerge() throws IOException{
//        if (mapOutputsFilesInMemory.size() == 0) {
//          return;
//        }
//        
//        //name this output file same as the name of the first file that is 
//        //there in the current list of inmem files (this is guaranteed to
//        //be absent on the disk currently. So we don't overwrite a prev. 
//        //created spill). Also we need to create the output file now since
//        //it is not guaranteed that this file will be present after merge
//        //is called (we delete empty files as soon as we see them
//        //in the merge method)
//
//        //figure out the mapId 
//        TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
//
//        List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K,V>>();
//        long mergeOutputSize = createInMemorySegments(inMemorySegments, 0);
//        int noInMemorySegments = inMemorySegments.size();
//
//        Path outputPath = mapOutputFile.getInputFileForWrite(mapId, 
//                          reduceTask.getTaskID(), mergeOutputSize);
//
//        Writer writer = 
//          new Writer(conf, rfs, outputPath,
//                     conf.getMapOutputKeyClass(),
//                     conf.getMapOutputValueClass(),
//                     codec, null);
//
//        RawKeyValueIterator rIter = null;
//        try {
//          LOG.info("Initiating in-memory merge with " + noInMemorySegments + 
//                   " segments...");
//          
//          rIter = Merger.merge(conf, rfs,
//                               (Class<K>)conf.getMapOutputKeyClass(),
//                               (Class<V>)conf.getMapOutputValueClass(),
//                               inMemorySegments, inMemorySegments.size(),
//                               new Path(reduceTask.getTaskID().toString()),
//                               conf.getOutputKeyComparator(), reporter,
//                               spilledRecordsCounter, null);
//          
//          if (combinerRunner == null) {
//            Merger.writeFile(rIter, writer, reporter, conf);
//          } else {
//            combineCollector.setWriter(writer);
//            combinerRunner.combine(rIter, combineCollector);
//          }
//          writer.close();
//
//          LOG.info(reduceTask.getTaskID() + 
//              " Merge of the " + noInMemorySegments +
//              " files in-memory complete." +
//              " Local file is " + outputPath + " of size " + 
//              localFileSys.getFileStatus(outputPath).getLen());
//        } catch (Exception e) { 
//          //make sure that we delete the ondisk file that we created 
//          //earlier when we invoked cloneFileAttributes
//          localFileSys.delete(outputPath, true);
//          throw (IOException)new IOException
//                  ("Intermediate merge failed").initCause(e);
//        }
//
//        // Note the output of the merge
//        FileStatus status = localFileSys.getFileStatus(outputPath);
//        synchronized (mapOutputFilesOnDisk) {
//          addToMapOutputFilesOnDisk(status);
//        }
//      }
//    }
//
//    private class GetMapEventsThread extends Thread {
//      
//      private IntWritable fromEventId = new IntWritable(0);
//      private static final long SLEEP_TIME = 1000;
//      
//      public GetMapEventsThread() {
//        setName("Thread for polling Map Completion Events");
//        setDaemon(true);
//      }
//      
//      @Override
//      public void run() {
//      
//        LOG.info(reduceTask.getTaskID() + " Thread started: " + getName());
//        
//        do {
//          try {
//            int numNewMaps = getMapCompletionEvents();
//            if (numNewMaps > 0) {
//              LOG.info(reduceTask.getTaskID() + ": " +  
//                  "Got " + numNewMaps + " new map-outputs"); 
//            }
//            Thread.sleep(SLEEP_TIME);
//          } 
//          catch (InterruptedException e) {
//            LOG.warn(reduceTask.getTaskID() +
//                " GetMapEventsThread returning after an " +
//                " interrupted exception");
//            return;
//          }
//          catch (Throwable t) {
//            String msg = reduceTask.getTaskID()
//                         + " GetMapEventsThread Ignoring exception : " 
//                         + StringUtils.stringifyException(t);
//            reportFatalError(getTaskID(), t, msg);
//          }
//        } while (!exitGetMapEvents);
//
//        LOG.info("GetMapEventsThread exiting");
//      
//      }
//      
//      /** 
//       * Queries the {@link TaskTracker} for a set of map-completion events 
//       * from a given event ID.
//       * @throws IOException
//       */  
//      private int getMapCompletionEvents() throws IOException {
//        
//        int numNewMaps = 0;
//        
//        MapTaskCompletionEventsUpdate update = 
//          umbilical.getMapCompletionEvents(reduceTask.getJobID(), 
//                                           fromEventId.get(), 
//                                           MAX_EVENTS_TO_FETCH,
//                                           reduceTask.getTaskID());
//        TaskCompletionEvent events[] = update.getMapTaskCompletionEvents();
//          
//        // Check if the reset is required.
//        // Since there is no ordering of the task completion events at the 
//        // reducer, the only option to sync with the new jobtracker is to reset 
//        // the events index
//        if (update.shouldReset()) {
//          fromEventId.set(0);
//          obsoleteMapIds.clear(); // clear the obsolete map
//          mapLocations.clear(); // clear the map locations mapping
//        }
//        
//        // Update the last seen event ID
//        fromEventId.set(fromEventId.get() + events.length);
//        
//        // Process the TaskCompletionEvents:
//        // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
//        // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop 
//        //    fetching from those maps.
//        // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
//        //    outputs at all.
//        for (TaskCompletionEvent event : events) {
//          switch (event.getTaskStatus()) {
//            case SUCCEEDED:
//            {
//              URI u = URI.create(event.getTaskTrackerHttp());
//              String host = u.getHost();
//              TaskAttemptID taskId = event.getTaskAttemptId();
//              int duration = event.getTaskRunTime();
//              if (duration > maxMapRuntime) {
//                maxMapRuntime = duration; 
//                // adjust max-fetch-retries based on max-map-run-time
//                maxFetchRetriesPerMap = Math.max(MIN_FETCH_RETRIES_PER_MAP, 
//                  getClosestPowerOf2((maxMapRuntime / BACKOFF_INIT) + 1));
//              }
//              URL mapOutputLocation = new URL(event.getTaskTrackerHttp() + 
//                                      "/mapOutput?job=" + taskId.getJobID() +
//                                      "&map=" + taskId + 
//                                      "&reduce=" + getPartition());
//              List<MapOutputLocation> loc = mapLocations.get(host);
//              if (loc == null) {
//                loc = Collections.synchronizedList
//                  (new LinkedList<MapOutputLocation>());
//                mapLocations.put(host, loc);
//               }
//              loc.add(new MapOutputLocation(taskId, host, mapOutputLocation));
//              numNewMaps ++;
//            }
//            break;
//            case FAILED:
//            case KILLED:
//            case OBSOLETE:
//            {
//              obsoleteMapIds.add(event.getTaskAttemptId());
//              LOG.info("Ignoring obsolete output of " + event.getTaskStatus() + 
//                       " map-task: '" + event.getTaskAttemptId() + "'");
//            }
//            break;
//            case TIPFAILED:
//            {
//              copiedMapOutputs.add(event.getTaskAttemptId().getTaskID());
//              LOG.info("Ignoring output of failed map TIP: '" +  
//                   event.getTaskAttemptId() + "'");
//            }
//            break;
//          }
//        }
//        return numNewMaps;
//      }
//    }
//  }
//
//  /**
//   * Return the exponent of the power of two closest to the given
//   * positive value, or zero if value leq 0.
//   * This follows the observation that the msb of a given value is
//   * also the closest power of two, unless the bit following it is
//   * set.
//   */
//  private static int getClosestPowerOf2(int value) {
//    if (value <= 0)
//      throw new IllegalArgumentException("Undefined for " + value);
//    final int hob = Integer.highestOneBit(value);
//    return Integer.numberOfTrailingZeros(hob) +
//      (((hob >>> 1) & value) == 0 ? 0 : 1);
//  }
//}
//
//class HCombinerRunner{
//	
}