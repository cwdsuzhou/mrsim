package hasim;

import org.apache.log4j.Logger;

import dfs.Pair;

import hasim.core.Datum;

import java.io.IOException;
import java.util.*;

public class HMerger {
	/**
	 * Logger for this class
	 */
	//private static final Logger logger = Logger.getLogger(HMerger.class);
	private static Logger logger ;

	static class Logger{
		public static void info(Object msg){
			System.out.println("HMerger log: "+ msg.toString());
		}
		
	}
	private static final float CHKSUM_AS_FRACTION = 0.01f;

	/**
	 * Determine the number of segments to merge in a given pass. Assuming more
	 * than factor segments, the first pass should attempt to bring the total
	 * number of segments - 1 to be divisible by the factor - 1 (each pass takes
	 * X segments and produces 1) to minimize the number of merges.
	 */
	static private int getPassFactor(int factor, int passNo, int numSegments) {
		if (passNo > 1 || numSegments <= factor || factor == 1)
			return factor;
		int mod = (numSegments - 1) % (factor - 1);
		if (mod == 0)
			return factor;
		return mod + 1;
	}

	public static long computeBytesInMerges(int factor, int inMem,
			List<Datum> segments) {
		return computeBytesInMerges(factor, inMem, segments, true);
	}

	/**
	 * Compute expected size of input bytes to merges, will be used in
	 * calculating mergeProgress. This simulates the above merge() method and
	 * tries to obtain the number of bytes that are going to be merged in all
	 * merges(assuming that there is no combiner called while merging).
	 * 
	 * @param factor
	 *            mapreduce.task.mapreduce.task.io.sort.factor
	 * @param inMem
	 *            number of segments in memory to be merged
	 */
	public static long computeBytesInMerges(int factor, int inMem,
			List<Datum> segments, boolean includeFinalMerge) {
		int numSegments = segments.size();
		List<Double> segmentSizes = new ArrayList<Double>(numSegments);
		long totalBytes = 0;
		int n = numSegments - inMem;
		// factor for 1st pass
		int f = getPassFactor(factor, 1, n) + inMem;
		n = numSegments;

		for (int i = 0; i < numSegments; i++) {
			// Not handling empty segments here assuming that it would not
			// affect
			// much in calculation of mergeProgress.
			segmentSizes.add(segments.get(i).size);
		}

		// If includeFinalMerge is true, allow the following while loop iterate
		// for 1 more iteration. This is to include final merge as part of the
		// computation of expected input bytes of merges
		boolean considerFinalMerge = includeFinalMerge;

		while (n > f || considerFinalMerge) {
			if (n <= f) {
				considerFinalMerge = false;
			}
			double mergedSize = 0;
			f = Math.min(f, segmentSizes.size());

			// logger.info("merg factor: "+ f);
			for (int j = 0; j < f; j++) {
				mergedSize += segmentSizes.remove(0);
			}
			totalBytes += mergedSize;

			// insert new size into the sorted list
			int pos = Collections.binarySearch(segmentSizes, mergedSize);
			if (pos < 0) {
				pos = -pos - 1;
			}
			segmentSizes.add(pos, mergedSize);

			n -= (f - 1);
			f = factor;
		}

		return totalBytes;
	}

	public static List<Pair<Integer, Double>> computeBytesInMergesList(
			int factor, int inMem, List<Datum> segments,
			boolean considerFinalMerge) {
		List<Pair<Integer, Double>> result = new ArrayList<Pair<Integer, Double>>();

		int numSegments = segments.size();
		List<Double> segmentSizes = new ArrayList<Double>(numSegments);
		long totalBytes = 0;
		int n = numSegments - inMem;
		// factor for 1st pass
		int f = getPassFactor(factor, 1, n) + inMem;
		n = numSegments;

		for (int i = 0; i < numSegments; i++) {
			// Not handling empty segments here assuming that it would not
			// affect
			// much in calculation of mergeProgress.
			segmentSizes.add(segments.get(i).size);
		}

		// If includeFinalMerge is true, allow the following while loop iterate
		// for 1 more iteration. This is to include final merge as part of the
		// computation of expected input bytes of merges
		// boolean considerFinalMerge = includeFinalMerge;

		while (n > f || considerFinalMerge) {
			if (n <= f) {
				considerFinalMerge = false;
			}
			double mergedSize = 0;
			f = Math.min(f, segmentSizes.size());

			// logger.info("merg factor: "+ f);
			for (int j = 0; j < f; j++) {
				mergedSize += segmentSizes.remove(0);
			}
			totalBytes += mergedSize;

			result.add(new Pair<Integer, Double>(f, mergedSize));

			// insert new size into the sorted list
			int pos = Collections.binarySearch(segmentSizes, mergedSize);
			if (pos < 0) {
				pos = -pos - 1;
			}
			segmentSizes.add(pos, mergedSize);

			n -= (f - 1);
			f = factor;
		}

		return result;
	}

	
	public static void mergeOrg(int factor, int inMem, double readsCounter,
			double writesCounter, List<Datum> segments,
			boolean considerFinalMerge) throws IOException {
		
		System.out.println("start new method");
		logger.info("Merging " + segments.size() + " sorted segments");

		// create the MergeStreams from the sorted map created in the
		// constructor
		// and dump the final output to a file
		int numSegments = segments.size();
		int origFactor = factor;
		int passNo = 1;
		do {
			System.out.println("passNo=1");
			// get the factor for this pass of merge. We assume in-memory
			// segments
			// are the first entries in the segment list and that the pass
			// factor
			// doesn't apply to them
			factor = getPassFactor(factor, passNo, numSegments - inMem);
			if (1 == passNo) {
				factor += inMem;
			}
			List<Datum> segmentsToMerge = new ArrayList<Datum>();
			int segmentsConsidered = 0;
			int numSegmentsToConsider = factor;
			long startBytes = 0; // starting bytes of segments of this merge
			double totalBytesProcessed = startBytes;
			double progPerByte=0, mergeProgress;

			while (true) {
				// extract the smallest 'factor' number of segments
				// Call cleanup on the empty segments (no key/value data)
				List<Datum> mStream = new ArrayList<Datum>();
				if (numSegmentsToConsider > segments.size()) {
					mStream = new ArrayList(segments);
					segments.clear();
				} else {

					mStream = new ArrayList(segments.subList(0,
							numSegmentsToConsider));
					for (int i = 0; i < numSegmentsToConsider; ++i) {
						segments.remove(0);
					}
				}

				for (int i = 0; i < mStream.size(); i++) {
					// Initialize the segment at the last possible moment;
					// this helps in ensuring we don't use buffers until we need
					// them
					// segment.init(readsCounter);
					Datum segment = mStream.get(i);
					boolean hasNext = (i < (mStream.size() - 1));
					startBytes += segment.size;

					if (hasNext) {
						segmentsToMerge.add(segment);
						segmentsConsidered++;
					} else {
						numSegments--; // we ignore this segment for the merge
					}
				}
				// if we have the desired number of segments
				// or looked at all available segments, we break
				if (segmentsConsidered == factor || segments.size() == 0) {
					break;
				}

				numSegmentsToConsider = factor - segmentsConsidered;
			}

			// feed the streams to the priority queue
			// initialize(segmentsToMerge.size());
			// clear();
			// for (Segment<K, V> segment : segmentsToMerge) {
			// put(segment);
			// }

			// if we have lesser number of segments remaining, then just return
			// the
			// iterator, else do another single level merge
			if (numSegments <= factor) {
				// Reset totalBytesProcessed to track the progress of the final
				// merge.
				// This is considered the progress of the reducePhase, the 3rd
				// phase
				// of reduce task. Currently totalBytesProcessed is not used in
				// sort
				// phase of reduce task(i.e. when intermediate merges happen).
				// totalBytesProcessed = startBytes;
				totalBytesProcessed = startBytes;
				// calculate the length of the remaining segments. Required for
				// calculating the merge progress
				long totalBytes = 0;
				for (int i = 0; i < segmentsToMerge.size(); i++) {
					totalBytes += segmentsToMerge.get(i).size;
				}
				if (totalBytes != 0) // being paranoid
					progPerByte = 1.0f / (float) totalBytes;

				if (totalBytes != 0)
					mergeProgress = (totalBytesProcessed * progPerByte);
				else
					mergeProgress = (1.0f); // Last pass and no segments left -
				// we're done

				logger.info("Down to the last merge-pass, with " + numSegments
						+ " segments left of total size: " + totalBytes
						+ " bytes");
				return ;//return
			} else {
				logger.info("Merging " + segmentsToMerge.size()
						+ " intermediate segments out of a total of "
						+ (segments.size() + segmentsToMerge.size()));

				// we want to spread the creation of temp files on multiple
				// disks if
				// available under the space constraints
				long approxOutputSize = 0;
				for (Datum s : segmentsToMerge) {
					approxOutputSize += s.size + s.size * CHKSUM_AS_FRACTION;
				}

				// we finished one single level merge; now clean up the priority
				// queue

				// Add the newly create segment to the list of segments to be
				// merged
				Datum tempSegment = new Datum("m_" + segments.size(),
						approxOutputSize, 0.0);
				segments.add(tempSegment);
				numSegments = segments.size();
				Collections.sort(segments, segmentComparator);

				passNo++;
			}
			// we are worried about only the first pass merge factor. So reset
			// the
			// factor to what it originally was
			factor = origFactor;
		} while (true);
	}

	static Comparator<Datum> segmentComparator = new Comparator<Datum>() {
		public int compare(Datum o1, Datum o2) {
			if (o1.size == o2.size) {
				return 0;
			}

			return o1.size < o2.size ? -1 : 1;
		}
	};
	

	public static void main(String[] args) {
		double records = 2621.44;
		logger.info("records " + records);

		double spills = 500000.0 / records;

		logger.info("spills " + spills);

		double size = records * 28;
		int intSpills = (int) Math.floor(spills);
		double remainSpill = spills - intSpills;

		List<Datum> list = new ArrayList<Datum>();
		for (int i = 0; i < intSpills; i++) {
			Datum d = new Datum("d_" + i, size, 0);
			list.add(d);
		}
		{
			Datum last = new Datum("last", size * remainSpill, 0);
			list.add(0, last);
			// list.add(last);//TODO either or before
		}

		List<Datum> queueList=new ArrayList<Datum>(list);
		
		double totalSize = size * spills / 2;

		double read = HMerger.computeBytesInMerges(10, 0, list) / 2;
		logger.info("read byte "+ read);
		double write = totalSize + read;

		List<Pair<Integer, Double>> result = HMerger.computeBytesInMergesList(
				10, 0, list, true);
		for (Pair<Integer, Double> pair : result) {
			logger.info(pair);
		}
		double totalSum = 0;
		for (Pair<Integer, Double> pair : result) {
			totalSum += pair.getV();
		}
		logger.info("read byte list" + (totalSum ));

		System.out.println("total size " + (long) totalSize);

		System.out.println("read " + (long) read);
		System.out.println("write " + (long) write);
		logger.info("" + (write / 2.00724908 * 2));
		
		
		logger.info("---------------------------------------------------");
		HMergeQueue queue=new HMergeQueue(queueList);
		//queue.merge(10, 2, 0, 0, null);
	}
}
