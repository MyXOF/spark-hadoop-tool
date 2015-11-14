package com.corp.tsdb.utils;

public class TypeWithWeight<T> implements Comparable<TypeWithWeight<T>> {
	public T key;
	public int weight;
	
	public TypeWithWeight(T key,int weight) {
		this.key = key;
		this.weight = weight;
	}
	
	@Override
	public int compareTo(TypeWithWeight<T> o) {
		return this.weight - o.weight;
	}

}
