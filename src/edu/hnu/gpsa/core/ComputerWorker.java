package edu.hnu.gpsa.core;

import java.io.IOException;
import java.util.BitSet;

import edu.hnu.gpsa.graph.Helper;
import kilim.Mailbox;
import kilim.Pausable;
import kilim.Task;

public class ComputerWorker extends Task {
	private static int counter = 0;
	private int cwid = counter++;
	private Handler handler;
	private Manager mgr;
	private BitSet firstMsg;
	private int nv;
	private int val;

	// Mailbox<Object> messages = new Mailbox<Object>(10000);
	BasicLongMailbox messages = new BasicLongMailbox(10000);

	public ComputerWorker(Handler handler, int nv, Manager mgr) {
		cwid = counter++;
		this.handler = handler;
		this.nv = nv;
		firstMsg = new BitSet(nv);
		this.mgr = mgr;

	}

	public void execute() throws Pausable, IOException {

		long msg = -1;
		msg = messages.get();

		int mVali = -1;

		int newVal;
		long offset = 0;
		int translateId = 0;
		int to = -1;
		int lastTo = -1;
		int lastVal = val;
		// System.out.println("before execute " + (val instanceof Long));

		while (msg != Signal.SYSTEM_OVER) {
			if (msg > 0) {
				to = Helper.getFirst(msg);
				mVali = Helper.getSecond(msg);
				translateId = translate(to);

				if (firstMsg.get(translateId)) {
					offset = index(to, 1);
				} else {
					offset = index(to, 0);
					// 这里不能将该标志设为true,因为后面写入数据事还要进行判断,再后面发生更新才会置入true
				}

				if (lastTo != to) {

					val = GlobalVaribaleManager.valMC.getInt(offset) & 0x7f_ff_ff_ff;

				} else {
					val = lastVal;
				}
				newVal = handler.compute(val, mVali);
				lastVal = newVal;
				lastTo = to;

				if (newVal != val) {
					if (firstMsg.get(translateId)) {
						System.out.println("1.write value for " + to + " at offset " + offset);
						writeValue(offset, newVal);
					} else {
						if (mgr.PINGPANG) {
							System.out.println("2.write value");
							writeValue(offset + 4, newVal);
						} else {
							System.out.println("3.write value");
							writeValue(offset - 4, newVal);
						}

						firstMsg.set(translateId);
					}
					lastVal = newVal;
				} else {
					if (!firstMsg.get(translateId)) {
						if (mgr.PINGPANG) {
							System.out.println("4.write value");
							writeNegValue(offset + 4, val);
						} else {
							System.out.println("5.write value");
							writeNegValue(offset - 4, val);
						}

					}
				}

				// System.out.println(msg + " ----- finish processing message "
				// + msg);
			} else if (msg == Signal.MANAGER_ITERATION_COMPUTE_OVER) {
				// 通知manager该compute worker上的计算操作已经完成

				mgr.noteCompute(Signal.COMPUTER_COMPUTE_OVER);
				firstMsg.clear();
			}
			// System.out.println("waitting messages from manager or dispatcher");
			msg = messages.get();

		}
	}

	private void writeValue(long offset, int newVal) {

		mgr.valMC.putInt(offset, newVal);

	}

	private void writeNegValue(long offset, int newVal) {

		mgr.valMC.putInt(offset, (newVal | 0x80_00_00_00));

	}

	private int translate(int to) {
		return to % nv;
	}

	public void iterationOver(long over) throws Pausable {
		messages.put(over);
	}

	public boolean putMsg(long msg) {
		return messages.putnb(msg);
	}

	public long index(int sequence, int type) {
		return mgr.index(sequence, type);
	}

}
