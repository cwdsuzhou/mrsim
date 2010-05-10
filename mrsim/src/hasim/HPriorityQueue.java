package hasim;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import hasim.core.Datum;



public class HPriorityQueue extends ArrayList<Datum>{
	static Comparator<Datum> segmentComparator = new Comparator<Datum>() {
		public int compare(Datum o1, Datum o2) {
			if (o1.size == o2.size) {
				return 0;
			}

			return o1.size < o2.size ? -1 : 1;
		}
	};
	
	  /** Determines the ordering of objects in this priority queue.  Subclasses
	      must define this one method. */
	  protected boolean lessThan(Datum a, Datum b){
		  return a.size < b.size;
		  
	  }

	  /**
	   * Adds an Object to a PriorityQueue in log(size) time.
	   * If one tries to add more objects than maxSize from initialize
	   * a RuntimeException (ArrayIndexOutOfBound) is thrown.
	   */
	  public final void put(Datum element) {
	    add(element);
	    Collections.sort(this, segmentComparator);
	  }

	  /**
	   * Adds element to the PriorityQueue in log(size) time if either
	   * the PriorityQueue is not full, or not lessThan(element, top()).
	   * @param element
	   * @return true if element is added, false otherwise.
	   */
	  public boolean insert(Datum element){
	    boolean result= add(element);
	    Collections.sort(this, segmentComparator);
	    return result;
	  }

	  /** Returns the least element of the PriorityQueue in constant time. */
	  public final Datum top() {
	    if (size() > 0)
	      return get(0);
	    else
	      return null;
	  }

	  /** Removes and returns the least element of the PriorityQueue in log(size)
	      time. */
	  public final Datum pop() {
	    if (size() > 0) {
	     return remove(0);
	    } else
	      return null;
	  }
	  
	  @Override
	public String toString() {
		return super.toString();
	}
	  public static void main(String[] args) {
			Datum d1=new Datum("d1 ", 10,0);
			Datum d2=new Datum("d2 ", 5,0);
			HPriorityQueue q=new HPriorityQueue();
			q.put(d2);
			q.put(d1);
			System.out.println(q);
			
			System.out.println(q.pop());
			System.out.println(q);
	}

}

