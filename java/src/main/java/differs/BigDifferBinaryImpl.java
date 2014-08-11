package differs;

import gnu.trove.list.linked.TLinkedList;
import gnu.trove.set.hash.THashSet;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bigdiff4j.MD5Wrapper;
import bigdiff4j.Main;
import bigdiff4j.Settings;

/**
 * BigDifferBinaryImpl
 * 
 * Implementation of the Big Differ, that stores the hashes in "buckets" as
 * binary data.
 * 
 * @author axiomzen
 *
 */
public class BigDifferBinaryImpl implements BigDiffer {

	private static final Logger logger = LogManager.getLogger(Main.class
			.getName());

	private static long time;

	private int hashSetSize = 30_000_000;

	private int bufferSize = 1048576;

	private String outpath = Settings.outputPath;

	private THashSet<MD5Wrapper> addedDiffs;
	private THashSet<MD5Wrapper> deletedDiffs;

	MD5Wrapper[][] prevArrays = new MD5Wrapper[2][];
	MD5Wrapper[][] currArrays = new MD5Wrapper[2][];

	private boolean nioMode = false;

	public void setOutputPath(String out) {
		outpath = out;

	}

	public BigDifferBinaryImpl() {
	}

	@Override
	public void bucketize(String prefix, String gzFilename) {
		BucketizerBinary bucketizer = new BucketizerBinary(outpath, prefix);
		bucketizer.bucketize(gzFilename);

	}

	@Override
	public void bigDiff(String prevPrefix, String currPrefix) {
		addedDiffs = new THashSet<MD5Wrapper>(hashSetSize);
		deletedDiffs = new THashSet<MD5Wrapper>(hashSetSize);

		for (int i = 0; i <= numOutputFiles; i++) {

			String hexString = Integer.toHexString(i);

			time = System.currentTimeMillis();
			System.out.println("Sorting and Comparing: " + hexString);
			File prevFile = new File(outpath + prevPrefix + hexString
					+ fileExtention);
			File currFile = new File(outpath + currPrefix + hexString
					+ fileExtention);

			MD5Wrapper[] prevArray = readIntoByteArray(prevFile);
			MD5Wrapper[] currArray = readIntoByteArray(currFile);

			System.out.println("File read for " + hexString + " took "
					+ ((System.currentTimeMillis() - time) / 1000) + "s");
			MD5Wrapper[] diffArray = compareByte(prevArray, currArray);

			for (MD5Wrapper line : diffArray) {
				if (line.isAdded()) {
					addedDiffs.add(line);
				} else {
					deletedDiffs.add(line);
				}
			}
			System.out.println("Sorting and Comparing " + hexString + " took "
					+ ((System.currentTimeMillis() - time) / 1000) + "s");

		}

	}

	@Override
	public void finalPass() {
		finalPass(Settings.gzippedTurtleFileOld, Settings.gzippedTurtleFileNew);
	}

	public void finalPass(String gzOldFile, String gzNewFile) {
		File fileDeletedResult = new File(outpath
				+ Settings.outputDiffDeletedFilename);
		File fileAddedResult = new File(outpath
				+ Settings.outputDiffAddedFilename);
		try {
			BufferedWriter deletedResultWriter = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(
							fileDeletedResult), gzStringEncoding), bufferSize);
			BufferedWriter addedResultWriter = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(fileAddedResult),
							gzStringEncoding), bufferSize);

			// Get all deleted lines
			System.out.println("Extracting Deleted diffs");

			Thread tDeleted = new Thread(new Runnable() {
				public void run() {
					new DiffLineExtractorBinary()
							.extractDiffLinesBinaryProducerConsumer(gzOldFile,
									deletedResultWriter, deletedDiffs);
				}
			});
			System.out.println("Extracting Added diffs");

			Thread tAdded = new Thread(new Runnable() {
				public void run() {
					new DiffLineExtractorBinary()
							.extractDiffLinesBinaryProducerConsumer(gzNewFile,
									addedResultWriter, addedDiffs);
				}
			});
			tDeleted.start();
			tAdded.start();
			try {
				tDeleted.join();
				tAdded.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Finished extracting Diffs");
			if (deletedResultWriter != null) {
				try {
					deletedResultWriter.close();
				} catch (IOException e) {
					logger.warn("Could not close file! " + fileDeletedResult.getAbsolutePath());
					e.printStackTrace();
				}
			}
			if (addedResultWriter != null) {
				try {
					addedResultWriter.close();
				} catch (IOException e) {
					logger.warn("Could not close file! " + fileAddedResult.getAbsolutePath());
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Private Functions
	 */

	private MD5Wrapper[] readIntoByteArray(File file) {
		if (nioMode) {
			return readIntoByteArrayNIO(file);
		} else {
			return readIntoByteArrayIO(file);
		}
	}

	/**
	 * readIntoByteArray
	 *
	 * This is the NIO version, using a MappedByteBuffer to read the file
	 * 
	 * @param file
	 * @return an array of MD5Wrappers (not sorted) constructed using the input
	 *         file
	 */

	private MD5Wrapper[] readIntoByteArrayNIO(File file) {
		// Set up readers and streams
		DataInputStream reader = null;
		FileInputStream is = null;
		FileChannel inChannel = null;

		MD5Wrapper[] result = null;
		int lineCount = 0;
		try {
			is = new FileInputStream(file);
			inChannel = is.getChannel();
			MappedByteBuffer buffer = inChannel.map(
					FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
			buffer.load();
			result = new MD5Wrapper[buffer.limit() / 16];
			boolean EOF = false;
			while (buffer.hasRemaining()) {

				try {
					result[lineCount++] = new MD5Wrapper(buffer.getLong(),
							buffer.getLong());
				} catch (BufferUnderflowException e) {
					EOF = true;
				}
			}
			inChannel.close();
		} catch (FileNotFoundException e) {
			logger.warn(file.getAbsolutePath() + " not found");
			e.printStackTrace();
		} catch (IOException e) {
			logger.warn(file.getAbsolutePath() + " IO Error", e);
			e.printStackTrace();
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
				if (is != null) {
					is.close();
				}
				if (inChannel != null) {
					inChannel.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * readIntoByteArray
	 *
	 * This is the standard Java IO version, using a MappedByteBuffer to read
	 * the file
	 * 
	 * @param file
	 * @return an array of MD5Wrappers (not sorted) constructed using the input
	 *         file
	 */

	private MD5Wrapper[] readIntoByteArrayIO(File file) {
		DataInputStream reader = null;
		// Count number of lines in the file
		int lineCount = 0;
		try {
			reader = new DataInputStream(new BufferedInputStream(
					new FileInputStream(file)));

			boolean EOF = false;
			while (!EOF) {

				try {
					reader.readLong();
					reader.readLong();
					lineCount++;
				} catch (EOFException e) {
					EOF = true;
				}
			}
		} catch (FileNotFoundException e) {
			logger.warn(file.getAbsolutePath() + " not found");
			e.printStackTrace();
		} catch (IOException e) {
			logger.warn(file.getAbsolutePath() + " IO Error", e);
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Create the array that will be returned
		MD5Wrapper[] result = new MD5Wrapper[lineCount];
		// Populate the array from the specified file
		lineCount = 0;
		try {
			reader = new DataInputStream(new BufferedInputStream(
					new FileInputStream(file)));
			boolean EOF = false;
			while (!EOF) {
				try {
					result[lineCount++] = new MD5Wrapper(reader.readLong(),
							reader.readLong());
				} catch (EOFException e) {
					EOF = true;
				}
			}
		} catch (FileNotFoundException e) {
			logger.warn(file.getAbsolutePath() + " not found");
			e.printStackTrace();
		} catch (IOException e) {
			logger.warn(file.getAbsolutePath() + " IO Error", e);
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	/**
	 * compareByte
	 * 
	 * The main component of the Sort and Compare step
	 * 
	 * Will take the two MD5Wrapper arrays given to it and sort both, then make
	 * a comparisson between the two arrays, returning the differences
	 * 
	 * @param prev
	 *            - The array with older data
	 * @param curr
	 *            - The array with newer data
	 * @return An array of differences between the two input arrays
	 */
	private MD5Wrapper[] compareByte(MD5Wrapper[] prev, MD5Wrapper[] curr) {
		// Sort the arrays
		Arrays.parallelSort(prev);
		Arrays.parallelSort(curr);
		System.out.println("Sort Finished in: "
				+ ((System.currentTimeMillis() - time) / 1000) + "s");
		TLinkedList<MD5Wrapper> temp = new TLinkedList<MD5Wrapper>();

		// The logic behind the determining which line is different between the
		// two arrays
		int i = 0, j = 0, max_i = prev.length, max_j = curr.length;
		while (i < max_i || j < max_j) {
			// Starts by checking if there are duplicates in the array (checks
			// against the next item, as the array is sorted)
			while (i + 1 < max_i && prev[i].equals(prev[i + 1])) {
				i++;
			}
			while (j + 1 < max_j && curr[j].equals(curr[j + 1])) {
				j++;
			}

			// Check if we've exausted the "prev" array, if so, add all
			// remaining items from the "curr" array onto the result array
			if (i >= max_i && j < max_j) {

				curr[j].setAdded(true);
				temp.add(curr[j++]);
			}
			// Check if we've exausted the "curr" array, if so, add all
			// remaining items from the "prev" array onto the result array
			else if (j >= max_j && i < max_i) {
				prev[i].setAdded(false);
				temp.add(prev[i++]);
			}
			// If the items in the current positions of both arrays are equal,
			// progress
			else if (prev[i].equals(curr[j])) {
				i++;
				j++;
			}
			// If item in "prev" array is less than that of the item in "curr"
			// array, add to results array
			else if (prev[i].compareTo(curr[j]) < 0) {
				prev[i].setAdded(false);
				temp.add(prev[i++]);
			}
			// If item in "curr" array is less than that of the item in "prev"
			// array, add to results array
			else if (prev[i].compareTo(curr[j]) > 0) {
				curr[j].setAdded(true);
				temp.add(curr[j++]);
			}
		}
		return temp.toArray(new MD5Wrapper[temp.size()]);
	}

	/*
	 * Accessor Functions
	 */

	public void setNIOMode(boolean nioModeOn) {
		nioMode = nioModeOn;
	}

}
