package edu.hnu.gpsa.graph;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

public class MapperCore {

	private List<ByteBuffer> chunks = new ArrayList<>();
	private final static long TWOGIG = Integer.MAX_VALUE;
	private long length;
	private File coreFile;
	private RandomAccessFile coreFileAccessor;

	public MapperCore(File file, long size) throws IOException {
		// This is a for testing - to avoid the disk filling up
		coreFile = file;
		// coreFile.deleteOnExit();
		// Now create the actual file
		coreFileAccessor = new RandomAccessFile(coreFile, "rw");
		FileChannel channelMapper = coreFileAccessor.getChannel();
		long nChunks = size / TWOGIG;
		if (nChunks > Integer.MAX_VALUE)
			throw new ArithmeticException("Requested File Size Too Large");
		length = size;
		long countDown = size;
		long from = 0;
		while (countDown > 0) {
			long len = Math.min(TWOGIG, countDown);
			ByteBuffer chunk = channelMapper.map(MapMode.READ_WRITE, from, len);
			chunks.add(chunk);
			from += len;
			countDown -= len;
		}
	}

	public byte[] get(long offset, int size) throws IOException {
		// Quick and dirty but will go wrong for massive numbers
		double a = offset;
		double b = TWOGIG;
		byte[] dst = new byte[size];
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offset - whichChunk * TWOGIG;
		// Data does not straddle two chunks
		try {
			if (TWOGIG - withinChunk > dst.length) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position((int) withinChunk);
				readBuffer.get(dst, 0, dst.length);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				int l2 = (int) dst.length - l1;
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position((int) withinChunk);
				readBuffer.get(dst, 0, l1);

				chunk = chunks.get((int) whichChunk + 1);
				readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position(0);
				try {
					readBuffer.get(dst, l1, l2);
				} catch (java.nio.BufferUnderflowException e) {
					throw e;
				}
			}
		} catch (IndexOutOfBoundsException i) {
			throw new IOException("Out of bounds");
		}
		return dst;
	}

	public void put(long offset, byte[] src) throws IOException {
		// Quick and dirty but will go wrong for massive numbers
		double a = offset;
		double b = TWOGIG;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offset - whichChunk * TWOGIG;
		// Data does not straddle two chunks
		try {
			if (TWOGIG - withinChunk > src.length) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer writeBuffer = chunk.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(src, 0, src.length);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				int l2 = (int) src.length - l1;
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer writeBuffer = chunk.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(src, 0, l1);

				chunk = chunks.get((int) whichChunk + 1);
				writeBuffer = chunk.duplicate();
				writeBuffer.position(0);
				writeBuffer.put(src, l1, l2);

			}
		} catch (IndexOutOfBoundsException i) {
			throw new IOException("Out of bounds");
		}
	}

	public int getInt(long offset) throws IOException {
		double a = offset;
		double b = TWOGIG;
		int val = 0;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offset - whichChunk * TWOGIG;
		// Data does not straddle two chunks
		try {
			if (TWOGIG - withinChunk > 4) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				val = chunk.getInt((int) withinChunk);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position((int) withinChunk);
				for (int i = 0; i < l1; i++) {
					val += ((readBuffer.get() & 0xff) << (24 - i * 8));
				}

				chunk = chunks.get((int) whichChunk + 1);
				readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position(0);

				for (int i = l1; i < 4; i++) {
					val += ((readBuffer.get() & 0xff) << (24 - i * 8));
				}

			}
		} catch (IndexOutOfBoundsException i) {
			System.out.println("----"+ offset);
			throw new IOException("Out of bounds");
		}
		return val;
	}

	public void putInt(long offset, int val) {
		double a = offset;
		double b = TWOGIG;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offset - whichChunk * TWOGIG;

		try {
			if (TWOGIG - withinChunk > 4) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				chunk.putInt((int) withinChunk, val);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer writeBuffer = chunk.duplicate();
				writeBuffer.position((int) withinChunk);
				for (int i = 0; i < l1; i++)
					writeBuffer.put((byte) ((val >>> (24 - i * 8)) & 0xff));

				chunk = chunks.get((int) whichChunk + 1);
				writeBuffer = chunk.duplicate();
				writeBuffer.position(0);
				for (int i = l1; i < 4; i++) {
					writeBuffer.put((byte) ((val >>> (24 - i * 8)) & 0xff));
				}
			}
		} catch (Exception e) {
			System.out.println(offset +" and the size of the file is" + length);
			e.printStackTrace();
		}
	}

	public void putFloat(long offset, float val) {
		int x = Float.floatToIntBits(val);
		putInt(offset, x);
	}

	public void putNegFloat(long offset, float val) {
		int x = Float.floatToIntBits(val);
		putInt(offset, (x | 0x80_00_00_00));
	}

	public float getFloat(long offset) throws IOException {
		int x = getInt(offset);
		return Float.intBitsToFloat(x);
	}

	public void putLong(long offset, long val) {
		double a = offset;
		double b = TWOGIG;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offset - whichChunk * TWOGIG;

		try {
			if (TWOGIG - withinChunk > 8) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				chunk.putLong((int) withinChunk, val);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer writeBuffer = chunk.duplicate();
				writeBuffer.position((int) withinChunk);
				for (int i = 0; i < l1; i++)
					writeBuffer.put((byte) ((val >>> (56 - i * 8)) & 0xff));

				chunk = chunks.get((int) whichChunk + 1);
				writeBuffer = chunk.duplicate();
				writeBuffer.position(0);
				for (int i = l1; i < 8; i++) {
					writeBuffer.put((byte) ((val >>> (56 - i * 8)) & 0xff));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long getLong(long offset) throws IOException {
		double a = offset;
		double b = TWOGIG;
		long val = 0;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offset - whichChunk * TWOGIG;
		// Data does not straddle two chunks
		try {
			if (TWOGIG - withinChunk > 8) {
				ByteBuffer chunk = chunks.get((int) whichChunk);
				val = chunk.getLong((int) withinChunk);
			} else {
				int l1 = (int) (TWOGIG - withinChunk);
				ByteBuffer chunk = chunks.get((int) whichChunk);
				// Allows free threading
				ByteBuffer readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position((int) withinChunk);
				for (int i = 0; i < l1; i++) {
					val += ((readBuffer.get() & 0xff) << (56 - i * 8));
				}

				chunk = chunks.get((int) whichChunk + 1);
				readBuffer = chunk.asReadOnlyBuffer();
				readBuffer.position(0);

				for (int i = l1; i < 8; i++) {
					val += ((readBuffer.get() & 0xff) << (56 - i * 8));
				}

			}
		} catch (IndexOutOfBoundsException i) {
			throw new IOException("Out of bounds");
		}
		return val;
	}

	public void putDouble(long offset, double val) {
		long x = Double.doubleToLongBits(val);
		putLong(offset, x);
	}

	public void putNegDouble(long offset,double val){
		long x = Double.doubleToLongBits(val);
		putLong(offset, (x | 0x80_00_00_00_00_00_00_00L));
	}
	public double getDouble(long offset) throws IOException {
		long x = getLong(offset);
		return Double.longBitsToDouble(x);
	}

	public void purge() {
		if (coreFileAccessor != null) {
			try {
				coreFileAccessor.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				coreFile.delete();
			}
		}
	}

	public long getSize() {
		return length;
	}

	@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
	private void clean(final MappedByteBuffer buffer) {
		AccessController.doPrivileged(new PrivilegedAction() {
			@Override
			public Object run() {
				try {
					Method getCleanerMethod = buffer.getClass().getMethod(
							"cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod
							.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}
}