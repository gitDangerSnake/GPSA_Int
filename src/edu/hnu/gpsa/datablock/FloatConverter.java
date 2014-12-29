package edu.hnu.gpsa.datablock;

public class FloatConverter implements BytesToValueConverter<Float> {

	@Override
	public int sizeOf() {
		return 4;
	}

	@Override
	public Float getValue(byte[] array) {
		int x = ((array[0] & 0xff) << 24) + ((array[1] & 0xff) << 16) + ((array[2] & 0xff) << 8) + (array[3] & 0xff);
		return Float.intBitsToFloat(x);
	}

	@Override
	public void setValue(byte[] array, Float val) {
		int x = Float.floatToIntBits(val);
		array[0] = (byte) ((x >>> 24) & 0xff);
		array[1] = (byte) ((x >>> 16) & 0xff);
		array[2] = (byte) ((x >>> 8) & 0xff);
		array[3] = (byte) ((x >>> 0) & 0xff);

	}

	@Override
	public Float getValue(byte[] array, int left, int right) {
		int x = ((array[left] & 0xff) << 24) + ((array[left+1] & 0xff) << 16) + ((array[left+2] & 0xff) << 8) + (array[left+3] & 0xff);
		return Float.intBitsToFloat(x);
	}

	
}
