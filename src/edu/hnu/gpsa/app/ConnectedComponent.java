package edu.hnu.gpsa.app;

import java.io.IOException;

import edu.hnu.gpsa.core.Handler;
import edu.hnu.gpsa.core.Manager;
import edu.hnu.gpsa.datablock.IntConverter;

public class ConnectedComponent {

	public static void main(String[] args) throws IOException {
		IntConverter ic = new IntConverter();
		Handler handler = new ConnectedComponentHandler();
//		Manager mgr = new Manager("/home/labserver/gpsa_test/cc/journal/journal", ic, null, ic, 256, 4096, 5, handler,false);
//		Manager mgr = new Manager("/home/labserver/gpsa_test/cc/google/google", ic, null, ic, 256, 4096, 5, handler,false);
//		Manager mgr = new Manager("/home/labserver/gpsa_test/cc/verify/verify", ic, null, ic, 2, 16, 10, handler,false);
		Manager mgr = new Manager("google",ic,null,ic,2,16,10,handler,false);
		mgr.run();
	}
}
