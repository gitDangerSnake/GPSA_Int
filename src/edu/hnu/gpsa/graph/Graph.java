package edu.hnu.gpsa.graph;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Stack;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.hnu.gpsa.datablock.BytesToValueConverter;

public class Graph<V, E, M> {

	public static Logger logger = Logger.getLogger("graph-processing...");
	public static int MAXID;
	public static int numEdges;

	/**
	 * enum type: 图的格式
	 * */
	public enum graphFormat {
		EDGELIST, ADJACENCY
	};

	/*
	 * 图的文件名
	 */
	private String graphFilename;
	private graphFormat format;
	private BytesToValueConverter<E> eConv;
	// private BytesToValueConverter<V> vConv;
	private EdgeProcessor<E> edgeProcessor;
	private byte[] edgeValueTemplate;
	private DataOutputStream shovelWriter;
	private boolean isOutDegreeMatters;

	public Graph(String graphFilename, String format,
			BytesToValueConverter<E> edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<V> verterxValueTypeBytesToValueConverter,
			BytesToValueConverter<M> msgValueTypeBytesToValueConverter,
			EdgeProcessor<E> edgeProcessor, boolean isOutDegreematters)
			throws FileNotFoundException {

		this.graphFilename = graphFilename;

		if (format.toLowerCase().equals("edgelist"))
			this.format = graphFormat.EDGELIST;
		else if (format.toLowerCase().equals("adjacency"))
			this.format = graphFormat.ADJACENCY;

		this.eConv = edgeValueTypeBytesToValueConverter;
		// this.vConv = verterxValueTypeBytesToValueConverter;
		this.edgeProcessor = edgeProcessor;

		this.shovelWriter = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.shovelFilename(graphFilename))));

		if (eConv != null) {
			edgeValueTemplate = new byte[eConv.sizeOf()];
		} else {
			edgeValueTemplate = new byte[0];
		}

		/*
		 * if (vConv != null) { vertexValueTemplate = new byte[vConv.sizeOf()];
		 * } else { vertexValueTemplate = new byte[0]; }
		 */

		this.isOutDegreeMatters = isOutDegreematters;
	}

	public String getGraphFilename() {
		return graphFilename;
	}

	/**
	 * 从Graph plain文件中读取数据并
	 * */
	public void preprocess() throws IOException {

		File memfile = new File(Filename.csrFilename(graphFilename));
		File infofile = new File(Filename.graphInfoFilename(graphFilename));
		if (!memfile.exists() || !infofile.exists()) {
			memfile.delete();
			infofile.delete();
			if (graphFilename.contains("twitter")) {
				readTwitter(graphFilename);
			} else {

				BufferedReader bReader = new BufferedReader(
						new InputStreamReader(new FileInputStream(new File(
								graphFilename))));
				String ln = null;
				int lnNum = 0;
				if (format == graphFormat.EDGELIST) {
					Pattern p = Pattern.compile("\\s");
					while ((ln = bReader.readLine()) != null) {
						if (ln.startsWith("#"))
							continue;
						lnNum++;
						if (lnNum % 5000000 == 0) {
							logger.info("Reading line : " + lnNum);
						}
						// String[] tokenStrings = ln.split("\\s");

						String[] tokenStrings = p.split(ln);

						if (tokenStrings.length == 2) {

							addEdge(Integer.parseInt(tokenStrings[0]),
									Integer.parseInt(tokenStrings[1]), null);

						} else if (tokenStrings.length == 3) {

							addEdge(Integer.parseInt(tokenStrings[0]),
									Integer.parseInt(tokenStrings[1]),
									tokenStrings[2]);
						}
					}

				} else if (format == graphFormat.ADJACENCY) {
					while ((ln = bReader.readLine()) != null) {
						// id,value : id,value->id,value->id,value
						lnNum++;
						if (lnNum % 1000000 == 0) {
							logger.info("Reading line : " + lnNum);
						}

					}

				}

				bReader.close();
				numEdges = lnNum;

				edgelist_process();
			}

		} else {
			BufferedReader br = new BufferedReader(new FileReader(infofile));
			String ln = br.readLine();
			String[] infos = ln.split("\\s");
			MAXID = Integer.valueOf(infos[0]);
			numEdges = Integer.valueOf(infos[1]);
			br.close();
		}
	}

	public void addEdge(int from, int to, String token) throws IOException {

		addToShovel(
				from,
				to,
				(edgeProcessor != null ? edgeProcessor.receiveEdge(from, to,
						token) : null));
	}

	private void addToShovel(int from, int to, E value) throws IOException {
		shovelWriter.writeLong(Helper.pack(from, to));
		if (eConv != null) {
			eConv.setValue(edgeValueTemplate, value);
			shovelWriter.write(edgeValueTemplate);
		}
		shovelWriter.flush();
	}

	public void edgelist_process() throws IOException {

		shovelWriter.close();
		/***************************************************************
		 * 计算出边上权重字节大小与顶点value的字节大小
		 **************************************************************/
		int sizeOfEdgeValue = (eConv != null ? eConv.sizeOf() : 0); // 边上的值的字节大小

		/*************************************************************
		 * 处理边的shovel文件
		 **************************************************************/

		File shovelFile = new File(Filename.shovelFilename(graphFilename));
		long[] edges = new long[(int) shovelFile.length()
				/ (8 + sizeOfEdgeValue)];
		byte[] edgeValues = new byte[edges.length * sizeOfEdgeValue];

		// 处理边
		BufferedDataInputStream in = new BufferedDataInputStream(
				new FileInputStream(shovelFile));
		for (int k = 0; k < edges.length; k++) {
			long l = in.readLong();
			edges[k] = l;
			in.readFully(edgeValueTemplate);
			System.arraycopy(edgeValueTemplate, 0, edgeValues, sizeOfEdgeValue
					* k, sizeOfEdgeValue);
		}

		// numEdges += edges.length;

		in.close();
		shovelFile.delete();
		Helper.quickSort(edges, edgeValues, sizeOfEdgeValue, 0,
				edges.length - 1);

		/******************************************************************************************************************
		 * 处理边的shovel文件
		 ******************************************************************************************************************/
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.csrFilename(graphFilename))));

		int curvid = -1;
		int currentSequence = 0;
		int isstart = 0;

		// 从边构建邻接表
		for (int s = 0; s < edges.length; s++) {

			int from = Helper.getFirst(edges[s]);

			if (from != curvid) {
				if (curvid == -1) {
					curvid = from;
					continue;
				} else {

					while (currentSequence < curvid) {
						dos.writeInt(-1);
						currentSequence++;
					}

					if (isOutDegreeMatters) {
						int outdegree = s - isstart;
						dos.writeInt(outdegree);
					}
					while (isstart < s) {
						int to = Helper.getSecond(edges[isstart]);
						if (to > MAXID)
							MAXID = to;
						dos.writeInt(to);
						isstart++;
					}

					dos.writeInt(-1);
					currentSequence++;
					curvid = from;
					if (curvid > MAXID)
						MAXID = curvid;
				}
			}

		}

		if (isstart < edges.length) {
			while (currentSequence < curvid) {
				dos.writeInt(-1);
				currentSequence++;
			}

			if (isOutDegreeMatters) {
				int outdegree = edges.length - isstart;
				dos.writeInt(outdegree);
			}

			while (isstart < edges.length) {
				dos.writeInt(Helper.getSecond(edges[isstart]));
				isstart++;
			}
			dos.writeInt(-1);
		}
		dos.flush();
		dos.close();

		BufferedWriter bw = new BufferedWriter(new FileWriter(
				Filename.graphInfoFilename(graphFilename)));
		bw.write(MAXID + " " + numEdges);
		bw.flush();
		bw.close();
	}

	public void readTwitter(String filename) throws IOException {
		int curvid = -1;
		int currentSequence = 0;
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(new File(filename))));
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(Filename.csrFilename(filename))));
		String line = null;
		int from, to;
		Pattern p = Pattern.compile("\\s");
		String[] edge = null;
		Stack<Integer> stack = new Stack<Integer>();
		int count = 0;
		while ((line = reader.readLine()) != null) {
			count++;
			if (count % 100000 == 0) {
				System.out.println("reading lines:" + 100000);
			}
			edge = p.split(line);
			from = Integer.valueOf(edge[0]);
			to = Integer.valueOf(edge[1]);

			if (from > MAXID)
				MAXID = from;
			if (to > MAXID)
				MAXID = to;

			if (isOutDegreeMatters) {

				if (curvid != from) {
					if (curvid == -1) {
						curvid = from;
						continue;
					} else {
						while (currentSequence < from) {
							dos.writeInt(-1);
							currentSequence++;
						}

						dos.writeInt(stack.size());
						while (!stack.isEmpty()) {
							dos.writeInt(stack.pop());
						}

						dos.writeInt(-1);
						currentSequence++;
						curvid = from;
					}

				}

				stack.push(to);

			} else {
				while (currentSequence < from) {
					dos.writeInt(-1);
					currentSequence++;
				}
				if (currentSequence == from) {
					dos.writeInt(to);
				} else {
					dos.writeInt(-1);
					currentSequence++;
					while (currentSequence < from) {
						dos.writeInt(-1);
						currentSequence++;
					}
					dos.writeInt(to);
				}
			}

		}
		numEdges = count;

		dos.writeInt(-1);
		dos.flush();
		dos.close();
		reader.close();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(
				Filename.graphInfoFilename(graphFilename)));
		bw.write(MAXID + " " + numEdges);
		bw.flush();
		bw.close();

	}

	public File getCSRFile() {
		return new File(Filename.csrFilename(graphFilename));
	}

	public int getNumEdges() {
		return numEdges;
	}

	public static void main(String[] args) throws IOException {
		Graph<Integer, Integer, Integer> graph = new Graph<Integer, Integer, Integer>(
				"google", "edgelist", null, null, null, null, false);
		// graph.readTwitter("/home/labserver/twitter/twitter");

		graph.preprocess();
		System.out.println("write data finished");

		RandomAccessFile raf = new RandomAccessFile(new File(
				Filename.csrFilename("google")), "rw");
		FileChannel fc = raf.getChannel();
		ByteBuffer bb = fc.map(MapMode.READ_ONLY, 0, raf.length());

		bb.position(0);
		int sequence = 0;
		boolean flag = true;
		System.out.print(sequence + ": [");
		while (bb.hasRemaining()) {
			int x = bb.getInt();
			if (x == -1) {
				flag = !flag;
				sequence++;
				if (flag) {
					System.out.print(sequence + ": [");
				} else {
					System.out.println("]");
				}
			} else {
				if (!flag) {
					System.out.print(sequence + ": [");
					flag = true;
				}
				System.out.print(x + ",");
			}
		}
		System.out.println(graph.getNumEdges());
		raf.close();

	}

}
