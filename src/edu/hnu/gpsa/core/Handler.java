package edu.hnu.gpsa.core;

public interface Handler {
	
	int init(int sequence);
	int compute(int val,int mVal);

}
