package com.corp.tsdb.utils;


public class ObjectCompareWithDate<K> implements Comparable<ObjectCompareWithDate<K>>{
	public String date;
	public K value;

	public ObjectCompareWithDate(String date,K value) {
		// TODO Auto-generated constructor stub
		this.date = date;
		this.value = value;
	}
	
	@Override
	public String toString(){
		return date+","+value;
	}
	
	@Override
	public int compareTo(ObjectCompareWithDate<K> o) {
		String values1[],values2[];
		int value_1 = 0,value_2 = 0;
		values1 = this.date.split("-");
		values2 = o.date.split("-");
		
		if(values1.length != values2.length){
			return values1.length - values2.length;
		}
		for(int i = 0 ; i < values1.length;i++){
			value_1 = value_1 * 100 + Integer.parseInt(values1[i]);
			value_2 = value_2 * 100 + Integer.parseInt(values2[i]);
		}
		return value_1 - value_2;
	}

}
