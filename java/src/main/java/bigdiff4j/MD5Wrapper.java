package bigdiff4j;

import gnu.trove.list.TLinkableAdapter;

/**
 * MD5Wrapper
 * 
 * For use with BigDiffer Hold two longs that represent a MD5 Hash Also has
 * functions to allow insertion into a Trove Linked list, as well as the
 * Comparable interface
 * 
 * @author kanzhang
 *
 */
public class MD5Wrapper extends TLinkableAdapter<MD5Wrapper> implements
		Comparable<MD5Wrapper> {

	// The two longs containing the MD5 Hash
	long leftLong;
	long rightLong;

	// Boolean to know if this is an added or deleted line
	boolean isAdded;

	public MD5Wrapper(long left, long right) {
		leftLong = left;
		rightLong = right;
	}

	public long getLeft() {
		return leftLong;
	}

	public long getRight() {
		return rightLong;
	}

	public void setLeft(long left) {
		leftLong = left;
	}

	public void setRight(long right) {
		rightLong = right;
	}

	public boolean isAdded() {
		return isAdded;
	}

	public void setAdded(boolean b) {
		isAdded = b;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (leftLong ^ (leftLong >>> 32));
		result = prime * result + (int) (rightLong ^ (rightLong >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MD5Wrapper other = (MD5Wrapper) obj;
		if (leftLong != other.leftLong)
			return false;
		if (rightLong != other.rightLong)
			return false;
		return true;
	}

	@Override
	public int compareTo(MD5Wrapper o) {
		long myLong = getLeft();
		long hisLong = o.getLeft();
		if (myLong == hisLong) {
			myLong = getRight();
			hisLong = o.getRight();
		}
		long result = myLong - hisLong;

		if (result > 0) {
			return 1;
		} else if (result == 0) {
			return 0;
		} else {
			return -1;
		}
	}

}
