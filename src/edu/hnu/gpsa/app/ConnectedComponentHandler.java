package edu.hnu.gpsa.app;

import edu.hnu.gpsa.core.Handler;

public class ConnectedComponentHandler implements Handler{


	@Override
	public int init(int sequence) {
		return sequence+1;
	}


	@Override
	public int compute(int val, int mVal) {
		return val > mVal ? mVal : val;
	}

}
