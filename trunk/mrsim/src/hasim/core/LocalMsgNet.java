package hasim.core;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalMsgNet {
	private static AtomicInteger ID = new AtomicInteger();

	public final int id;
	public final double size;
	public final int from, to;
	public final int user;
	public final int returnTag;
	public final Object object;

//	private double startTime;
//	public double getStartTime() {
//		return startTime;
//	}
//
//	public void setStartTime(double startTime) {
//		this.startTime = startTime;
//	}
//
//	public double getEndTime() {
//		return endTime;
//	}
//
//	public void setEndTime(double endTime) {
//		this.endTime = endTime;
//	}
//
//	private double endTime;

	

	public LocalMsgNet(double size, int from, int to,
			int user, int returnTag, Object object) {
		this.id = ID.incrementAndGet();
		this.from=from;
		this.to=to;
		this.size = size;
		this.user = user;
		this.returnTag = returnTag;
		this.object = object;
	}

	@Override
	public String toString() {
		return "LocalMsg" + id + ", totalTime:" + size + " ," +
				" from:" + from+"->"+to+", "+object;
	}
}
