package hasim;

import java.util.ArrayList;
import java.util.Collection;

import eduni.simjava.Sim_system;

@SuppressWarnings("serial")
public class CircularList<E> {
	
	private ArrayList<E> list;
	private int index=0;

	

	public CircularList(Collection<E> c) {
		list=new ArrayList<E>(c);
	}
	public CircularList(int i) {
		list=new ArrayList<E>(i);
	}
	public CircularList() {
		this(50);
	}

	
	public void add(E e) {
		list.add(e);
	};
	public void add(E ...e){
		for (E e2 : e) {
			list.add(e2);
		}
	}
	
	public boolean contains(Object o){
		return list.contains(o);
	}
	public int size(){
		return list.size();
	}
	
	
	public E next() {
		int size=list.size();
		if(size == 0)return null;
//		if(size == 1)return list.get(0);
		
		E result=list.get(index);
		index = (index+1)% size;
		return result;
	}
	
	public boolean remove(Object o) {
		int pos=list.indexOf(o);
		
		if(pos==-1) return false;
		
		list.remove(pos);
		
		if(index < pos ) {
			return true;
		}
		if( index > pos){
			index--;
			return true;
		}
		if ( index == list.size() ){
			index=0;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer("size:"+ list.size());
		for (E e : list) {
			sb.append("\n"+e);
		}
		return sb.toString();
	}
	public static void main(String[] args) {
		CircularList<Integer> list=new CircularList<Integer>();
		//list.add(1, 2,3,4,5,6,7);
		list.add(1,3);
		for (int i = 0; i < 13; i++) {
			System.out.println(list.next());
		}
		int r=3;
		System.out.println("size before "+ list.size());

		System.out.println("remove "+ r+ " "+ list.remove(r));
		System.out.println("size after "+ list.size());
		System.out.println(list.next());
		System.out.println(list.next());
		System.out.println(list.next());
		System.out.println(list.next());
		System.out.println(list.next());
	}
}
