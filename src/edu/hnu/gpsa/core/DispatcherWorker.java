package edu.hnu.gpsa.core;

import java.io.IOException;

import edu.hnu.gpsa.graph.Helper;
import kilim.Pausable;
import kilim.Task;

public class DispatcherWorker extends Task {

	private static int counter = 0;
	private int did = counter++;

	private long currentoffset;
	private SequenceInterval interval;
	private int val;

	BasicLongMailbox signals = new BasicLongMailbox(3);

	private int sequence;
	private Handler handler;
	private Manager mgr;
	boolean isOutdegreeMatters;

	public DispatcherWorker(SequenceInterval interval, Handler handler, boolean isOutdegreeMatters, Manager mgr) {
		if (interval != null) {
			this.interval = interval;
			sequence = interval.start;
			currentoffset = interval.startOffset;
		}
		this.mgr = mgr;
		this.handler = handler;
		this.isOutdegreeMatters = isOutdegreeMatters;
	}

	public void offsetIncrement() {
		currentoffset += 4;
	}

	public void offsetReset() {
		currentoffset = interval.startOffset;
	}

	public void sequenceIncrement() {
		++sequence;
	}

	public void sequenceReset() {
		sequence = interval.start;

	}

	public void restAndStart() {
		sequenceReset();
		offsetReset();
	}

	public int getLastValue(int currentSequence) throws IOException{
		long offset = index(currentSequence,1);
		return GlobalVaribaleManager.valMC.getInt(offset) & 0x7f_ff_ff_ff;
	}
	public void execute() throws Pausable, IOException {
		long msg;
		int dest = -1;
		int vid = -1;
		long s = signals.get();
		long offset = -1;
		while (s != Signal.SYSTEM_OVER) {

			if (interval != null) {
				restAndStart();
				while (sequence < interval.end && currentoffset < interval.endOffset) {
					offset = index(sequence,0);
					
					getValue(offset);
				
					if (val < 0  ){

						while (currentoffset < interval.endOffset && (vid = GlobalVaribaleManager.csrMC.getInt(currentoffset)) != -1) {
							offsetIncrement();
						}
						while (currentoffset < interval.endOffset && (vid = GlobalVaribaleManager.csrMC.getInt(currentoffset)) == -1) {
							offsetIncrement();
							sequenceIncrement();
						}
						

					} else {// 数据发生了更新

						while (currentoffset < interval.endOffset && (vid = GlobalVaribaleManager.csrMC.getInt(currentoffset)) != -1) {
							msg = Helper.pack(vid, val);
							dest = locate(vid);
							System.out.println(sequence +"->" + vid +" : ["+msg+"]" + val);
							mgr.send(dest, msg);
							offsetIncrement();
						}
						
						disableValue(offset);

						while (currentoffset < interval.endOffset && (vid = GlobalVaribaleManager.csrMC.getInt(currentoffset)) == -1) {
							offsetIncrement();
							sequenceIncrement();
						}
					}
					
				}
			}

			// System.out.println("current iteration dispatch finished notify manager"
			// );
			// 通知manager，本迭代的分发任务完成
			mgr.noteDispatch(Signal.DISPATCHER_ITERATION_DISPATCH_OVER);

			// 在通知完manager后，到收到来自manager的通知之前，会有一段空闲时间，这里可以添加一些清理或者监控的功能，达到最大的CPU利用率

			if(zeroIte) zeroIte = false;
			s = signals.get();

		}

	}

	private void disableValue(long offset){
		GlobalVaribaleManager.valMC.putInt(offset, val | 0x80_00_00_00);
	}

	boolean zeroIte = true;

	public void getValue(long offset) throws IOException {
		// 获取当前sequence的value值

		if (zeroIte) {
			val = GlobalVaribaleManager.valMC.getInt(offset) & 0x7f_ff_ff_ff;
		} else
			val = GlobalVaribaleManager.valMC.getInt(offset);
	}

	public int locate(int id) {
		return id * Manager.ncomputer / (Manager.maxid + 1);
	}

	public void putSignal(long managerIterationStart) throws Pausable {
		signals.put(managerIterationStart);
	}

	public long index(int vid, int type) {
		return mgr.index(vid, type);
	}

}
