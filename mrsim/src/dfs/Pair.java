package dfs;


public class Pair<K,V>{
	K k;
	V v;
	public Pair(K k, V v) {
		this.k=k;
		this.v=v;
	}
	
	
	public K getK() {
		return k;
	}


	public void setK(K k) {
		this.k = k;
	}


	public V getV() {
		return v;
	}


	public void setV(V v) {
		this.v = v;
	}


	@Override
	public String toString() {
		return ""+k+"\t"+v+"";
	}
	
	@Override
	public boolean equals(Object obj) {
		if( obj == null)return false;
		Pair o=(Pair<K, V>)obj;
		return k==o.k && v==o.v;
	}
	
	@Override
	public int hashCode() {
		return k.hashCode()+31*v.hashCode();
	}
	
	
}