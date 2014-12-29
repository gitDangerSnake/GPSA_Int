package edu.hnu.gpsa.graph;

public interface EdgeProcessor<EdgeValueType> {
	EdgeValueType receiveEdge(int from ,int to , String token);
}
