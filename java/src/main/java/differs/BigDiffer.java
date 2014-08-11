package differs;

public interface BigDiffer {

	public static final int numOutputFiles = 0xff;

	public static final String prevFilePrefix = "old_";
	public static final String currFilePrefix = "new_";
	public static final String fileExtention = ".txt";
	public static final String gzStringEncoding = "UTF-8";

	/**
	 * initializeBucketWriters
	 * 
	 * NO LONGER NEEDED
	 * 
	 * Initializes the bucket writers that will be used by this implementation.
	 * The prefix will be used as the prefix of the file names of all buckets
	 * writen to by these bucket writers
	 * 
	 * @param prefix
	 * @return
	 */
	// ArrayList initializeBucketWriters(String prefix);

	/**
	 * bucketize
	 * 
	 * Should be called after initializeBucketWriters Sorts the gzFilename file
	 * into different buckets
	 * 
	 * @param gzFilename
	 */
	void bucketize(String prefix, String gzFilename);

	/**
	 * bigDiff
	 * 
	 * Calculates and stores the differences in the two GZ files, by sorting
	 * through the corresponding buckets created by bucketize
	 * 
	 * @param prevPrefix
	 * @param currPrefix
	 */
	void bigDiff(String prevPrefix, String currPrefix);

	/**
	 * Goes through the GZ file and pulls the lines that are different based on
	 * the bigDiff step
	 */
	void finalPass();

}
