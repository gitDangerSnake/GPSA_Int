package edu.hnu.gpsa.graph;

public class Filename {

	
	/*
	 * 图的输入格式EdgeList时，预处理过程边的shovel中间文件->该文件是紧凑的边二进制文件
	 */
	public static String shovelFilename(String graphfile){
		return graphfile+".shovel";
	}
	
	
	/*
	 * 图的邻接表结构二进制内存映射文件
	 */
	public static String csrFilename(String graphfile){
		return graphfile+".csr.mem";
	}
	
	public static String graphInfoFilename(String graphFile){
		return graphFile + ".info";
	}
	
	/*
	 * 图的顶点value的内存映射文件
	 */
	public static String vertexValueFilename(String graphfile){
		return graphfile + "v.mem";
	}
	
	/*
	 * 计算过程中的某个超级步的消息备份文件，该文件的主要作用是用来保证程序的容错性
	 * 如果程序意外终止，可以从邻接表文件、value文件与该文件中恢复执行
	 */
	public static String msgTmpFilename(String graphfile,int superstep){
		return graphfile+".msg"+superstep+".bak";
	}
	
}
