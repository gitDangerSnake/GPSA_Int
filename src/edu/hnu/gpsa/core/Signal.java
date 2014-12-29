package edu.hnu.gpsa.core;

public interface Signal {
	// can not use -1 for signal 
	static final long MANAGER_ITERATION_START = 1;
	static final long MANAGER_ITERATION_OVER = 0;
	static final long MANAGER_ITERATION_COMPUTE_OVER = -2;
	static final long DISPATCHER_ITERATION_DISPATCH_OVER = 3;
	static final long COMPUTER_COMPUTE_OVER = 4;
	static final long SYSTEM_OVER = Long.MIN_VALUE;

}
