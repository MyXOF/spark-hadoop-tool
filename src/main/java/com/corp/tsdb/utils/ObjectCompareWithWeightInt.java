package com.corp.tsdb.utils;

public class ObjectCompareWithWeightInt<T> implements Comparable<ObjectCompareWithWeightInt<T>> {
	public T key;
	public int weight;
	
	public ObjectCompareWithWeightInt(T key,int weight) {
		this.key = key;
		this.weight = weight;
	}
	
	@Override
	public int compareTo(ObjectCompareWithWeightInt<T> o) {
		return this.weight - o.weight;
	}

}
