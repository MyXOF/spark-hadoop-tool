package com.corp.tsdb.utils;

public class ObjectCompareWithWeightLong<T> implements Comparable<ObjectCompareWithWeightLong<T>> {
	public T key;
	public long weight;
	
	public ObjectCompareWithWeightLong(T key,long weight) {
		this.key = key;
		this.weight = weight;
	}
	
	
	@Override
	public int compareTo(ObjectCompareWithWeightLong<T> o) {
		// TODO Auto-generated method stub
		if(this.weight < o.weight) return -1;
		else if (this.weight > o.weight) return 1;
		return 0;
	}

}
