package edu.umich.eecs.featext.index;

import java.nio.ByteBuffer;

public class FileOffset {
	private long offset;
	private int length;
	
	public FileOffset() {}
	
	public FileOffset(long off, int len) {
		this.offset = off;
		this.length = len;
	}
	
	public FileOffset(byte[] b) {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.put(b);
		this.offset = bb.getLong(0);
		this.length = bb.getInt(8);
	}
	
	public byte[] toBytes() {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putLong(0, this.offset);
		bb.putInt(8, this.length);
		return bb.array();
	}
	
	public int getLength() {
		return this.length;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public String toString() {
		return "<" + this.offset +", " + this.length + ">";
	}
	
	
	// helper method to pack 2 ints into a long
	public static long packIntsInLong(int x, int y) {
		return (((long)x) << 32) | (y & 0xffffffffL);
	}
	
	// get our 2 ints back out of the long
	public static int[] unpackLongToInts(long l) {
		int[] ints = new int[2];
		ints[0] = (int)(l >> 32);
		ints[1] = (int)l;
		return ints;
	}
	
	public static void main(String[] args) {
		long l = 1234567890987654321L;
		int i = 445566;
		
		FileOffset fo = new FileOffset(l, i);
		byte[] b = fo.toBytes();
		FileOffset newFo = new FileOffset(b);
		System.out.println(fo + " -> " + b + " -> " + newFo);
		
	}

}
