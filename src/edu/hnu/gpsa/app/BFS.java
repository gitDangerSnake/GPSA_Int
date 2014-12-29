package edu.hnu.gpsa.app;

import java.io.IOException;

import edu.hnu.gpsa.core.GlobalVaribaleManager;
import edu.hnu.gpsa.core.Handler;
import edu.hnu.gpsa.core.Manager;
import edu.hnu.gpsa.datablock.IntConverter;

public class BFS {

	public static void main(String[] args) throws IOException {
		IntConverter ic = new IntConverter();
		Handler handler = new BFSHandler();
//		Manager mgr = new Manager("/home/labserver/gpsa_test/bfs/journal/journal", ic, null, ic, 256, 4096, 5, handler,false);
//		Manager mgr = new Manager("/home/labserver/gpsa_test/bfs/google/google", ic, null, ic, 256, 4096, 5, handler,false);
//		Manager mgr = new Manager("/home/labserver/gpsa_test/bfs/verify/verify", ic, null, ic, 2, 16, 10, handler,false);
		Manager mgr = new Manager("google", ic, null, ic, 1, 1, 15, handler,false);
		mgr.run();
		
	}
}
