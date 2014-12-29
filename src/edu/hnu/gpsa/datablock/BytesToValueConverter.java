
package edu.hnu.gpsa.datablock;
public interface BytesToValueConverter<T> {
	public int sizeOf();
	public T getValue(byte[] array);
	public T getValue(byte[] array,int left,int right); //[left,right)
	public void setValue(byte[] array,T val);

}
