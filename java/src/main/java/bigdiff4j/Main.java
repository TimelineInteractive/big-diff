package bigdiff4j;

import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import differs.BigDiffer;
import differs.BigDifferBinaryImpl;

/**
 * Java version of the "Big Diff"
 * 
 * Three Stages: 1. Bucketize - Seperates the input files into different
 * "bucket" files on disk 2. Sort and Compare - Goes through the buckets and
 * sorts then compares the corresponding buckets between data dumps 3. Diff
 * Extraction - Reads through the data dump rdf files again and extracts the
 * lines that are different
 * 
 * @author axiomzen
 *
 */
public class Main {

	// log4j Logger
	private static final Logger logger = LogManager.getLogger(Main.class
			.getName());

	// Messages saved for printing at end of program execution
	private static LinkedList<String> messages = new LinkedList<String>();

	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		logger.trace("Start big diff:" + time);
		messages.add("Start big diff:" + time);

		// Currently using the implementation where the buckets store binary
		// data, rather than string data
		BigDiffer differ = new BigDifferBinaryImpl();

		// Bucketize
		// ==========
		// Create a thread for bucketizing the older gzipped archive
		Thread tPrev = new Thread(new Runnable() {
			public void run() {
				differ.bucketize(BigDiffer.prevFilePrefix,
						Settings.gzippedTurtleFileOld);
			}
		});

		// Create a thread for bucketizing the newer gzipped archive
		Thread tCurrent = new Thread(new Runnable() {
			public void run() {
				differ.bucketize(BigDiffer.currFilePrefix,
						Settings.gzippedTurtleFileNew);
			}
		});

		// Start up both threads and wait for them to finish before we continue
		tPrev.start();
		tCurrent.start();
		try {
			tPrev.join();
			tCurrent.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.trace("Done bucketize: " + (System.currentTimeMillis() - time));
		messages.add("Done bucketize: " + (System.currentTimeMillis() - time));
		System.out.println("Done bucketize: "
				+ (System.currentTimeMillis() - time));

		// Sort and Compare
		// ================
		// Run the differ to extract the hashes that are different between the
		// two archives
		differ.bigDiff(BigDiffer.prevFilePrefix, BigDiffer.currFilePrefix);

		messages.add("Done Big Diff: " + (System.currentTimeMillis() - time));
		System.out.println("Done Big Diff: "
				+ (System.currentTimeMillis() - time));

		// Diff Extraction
		// ===============
		// Extract the actual lines of difference from the two archives
		differ.finalPass();

		logger.trace("Total time taken: "
				+ ((System.currentTimeMillis() - time) / 1000) + "s");
		messages.add("Total time taken: "
				+ ((System.currentTimeMillis() - time) / 1000) + "s");

		// Print out all messages that have been saved
		for (String message : messages) {
			System.out.println(message);
		}

	}

}
