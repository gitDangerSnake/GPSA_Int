package edu.hnu.gpsa.app;

import edu.hnu.gpsa.core.Handler;

public class BFSHandler implements Handler{

	@Override
	public int init(int sequence) {
		if(sequence == 0) return 1;
		else return Integer.MAX_VALUE;
	}

	@Override
	public int compute(int val, int mVal) {
		return val > mVal ? mVal + 1 : val;
	}


}
