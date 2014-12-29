package edu.hnu.gpsa.core;


import edu.hnu.gpsa.graph.MapperCore;

public class GlobalVaribaleManager {

	protected static MapperCore csrMC;
	protected static MapperCore valMC;
	public static void init(MapperCore csrMC2, MapperCore valMC2) {
		csrMC= csrMC2;
		valMC = valMC2;
	}


}
