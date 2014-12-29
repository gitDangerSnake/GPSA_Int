package edu.hnu.gpsa.datablock;

public class IntConverter implements BytesToValueConverter<Integer> {

	@Override
	public int sizeOf() {
		return 4;
	}

	@Override
	public Integer getValue(byte[] array) {
		return ((array[0] & 0xff) << 24) + ((array[1] & 0xff) << 16) + ((array[2] & 0xff) << 8) + (array[3] & 0xff);
	}

	@Override
	public void setValue(byte[] array, Integer val) {
		array[3] = (byte) ((val) & 0xff);
		array[2] = (byte) ((val >>> 8) & 0xff);
		array[1] = (byte) ((val >>> 16) & 0xff);
		array[0] = (byte) ((val >>> 24) & 0xff);
	}

	@Override
	public Integer getValue(byte[] array, int left, int right) {
		assert (left+3) == right;
		return ((array[left] & 0xff) << 24) + ((array[left+1] & 0xff) << 16) + ((array[left+2] & 0xff) << 8) + (array[left+3] & 0xff);
		
	}
	
	
	public void setValue(byte[] array,Integer val,int start){
		array[start+3] = (byte) ((val) & 0xff);
		array[start+2] = (byte) ((val >>> 8) & 0xff);
		array[start+1] = (byte) ((val >>> 16) & 0xff);
		array[start] = (byte) ((val >>> 24) & 0xff);
	}

}
