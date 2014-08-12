package differs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.axiomzen.util.Brokerable;

import bigdiff4j.Main;
import bigdiff4j.Settings;

/**
 * BigDifferStringImpl
 * 
 * Implementation of the Big Differ, that stores the hashes in "buckets" as hex
 * strings.
 * 
 * CURRENTLY UNMAINTAINED
 * 
 * @author axiomzen
 *
 */
public class BigDifferStringImpl implements BigDiffer {

	private BufferedReader in;
	private GZIPInputStream is;
	private MessageDigest md;

	private long count = 0;

	private static final Logger logger = LogManager.getLogger(Main.class
			.getName());

	final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

	private ArrayList<BufferedWriter> outputFiles = new ArrayList<BufferedWriter>(
			numOutputFiles);

	private BufferedWriter bigDiffResultWriter;
	private BufferedWriter bigDiffDeletedWriter;
	private BufferedWriter bigDiffAddedWriter;

	private String encoding = "ASCII";

	/**
	 * initializeBucketWriters
	 * 
	 * @param prefix
	 *            - prefix of the buckets (commonly "old_" and "new_")
	 */
	public void initializeBucketWriters(String prefix) {

		for (int i = 0; i <= numOutputFiles; i++) {
			String hexString = Integer.toHexString(i);
			File file = new File(Settings.outputPath + prefix + hexString
					+ fileExtention);
			OutputStreamWriter osWriter = null;
			BufferedWriter bWriter = null;

			try {
				osWriter = new OutputStreamWriter(new FileOutputStream(file),
						encoding);
			} catch (IOException e) {
				e.printStackTrace();
			}
			bWriter = new BufferedWriter(osWriter);
			outputFiles.add(i, bWriter);
		}

		// Registers a shutdown hook for the buffered writters instance so that
		// it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (BufferedWriter writer : outputFiles) {
					if (writer != null) {
						try {
							writer.flush();
							writer.close();
						} catch (IOException e) {
							logger.error("Error Closing Writer", e);
						}

					}
				}
			}
		});

	}

	/**
	 * closeOutputFiles
	 * 
	 * Used to close all open bucket file writers
	 */
	private void closeOutputFiles() {
		for (BufferedWriter writer : outputFiles) {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					logger.error("Error Closing Writer", e);
				}
			}
		}
	}

	@Override
	public void bucketize(String prefix, String gzFilename) {
		initializeBucketWriters(prefix);
		com.axiomzen.util.BlockingQueueBroker<String> broker1 = new com.axiomzen.util.BlockingQueueBroker<String>(
				500);
		final long time = System.currentTimeMillis();
		count = 0;
		// Create Consumer
		ExecutorService threadPool = Executors.newFixedThreadPool(2);
		threadPool.execute(new com.axiomzen.util.Consumer<String>("", broker1) {

			@Override
			public void consume(String data) {
				if (data != null) {
					try {
						processLine(data);
						count++;
						if (count % 100_000 == 0)
							System.out.print(".");
						if (count % 1_000_000 == 0) {
							System.out.println(count
									/ 1_000_000
									+ "m; "
									+ ((System.currentTimeMillis() - time) / 1000)
									+ "s");
						}
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
			}
		});

		// Create Producer
		java.util.concurrent.Future<?> producerStatus = threadPool
				.submit(new com.axiomzen.util.Producer<String>(broker1) {

					@Override
					public void produce(Brokerable<String> broker)
							throws InterruptedException {

						try {
							logger.trace("Opening zip file");
							is = new GZIPInputStream(new FileInputStream(
									gzFilename));
						} catch (FileNotFoundException e) {
							logger.warn(gzFilename + " not found");
							e.printStackTrace();
							return;
						} catch (IOException e) {
							logger.warn(gzFilename + " IO error", e);
							e.printStackTrace();
							return;
						}
						logger.trace("Opening buffered reader");
						in = new BufferedReader(new InputStreamReader(is));
						//
						String line = null;
						try {
							logger.trace("Reading Zip File");
							while ((line = in.readLine()) != null) {
								broker.put(line);
							}
						} catch (IOException e) {
							logger.error(e.getLocalizedMessage()
									+ e.getStackTrace());
						} finally {
							try {
								if (in != null)
									in.close();
							} catch (IOException ex) {
								ex.printStackTrace();
							}
						}
					}
				});

		// Wait for Producer to finish
		try {
			logger.trace("Waiting for producer (part 1) to finish");
			producerStatus.get();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			e1.printStackTrace();
		}

		logger.trace("Producer finished, shutting down threadPool1");
		threadPool.shutdown();
		try {
			logger.trace("Waiting for termination of threadPool1");
			if (threadPool.awaitTermination(60, TimeUnit.MINUTES)) {
				logger.trace("threadPool1 terminated sucessfully");
			} else {
				logger.warn("threadPool1 did not terminate in time allotted, this could be a problem");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		closeOutputFiles();

	}

	@Override
	public void bigDiff(String prevPrefix, String currPrefix) {
		File fileDeletedDiff = new File(Settings.outputPath
				+ Settings.outputDiffDeleted);
		File fileAddedDiff = new File(Settings.outputPath
				+ Settings.outputDiffAdded);
		OutputStreamWriter osDeletedWriter = null;
		OutputStreamWriter osAddedWriter = null;
		try {
			osDeletedWriter = new OutputStreamWriter(new FileOutputStream(
					fileDeletedDiff), encoding);
			osAddedWriter = new OutputStreamWriter(new FileOutputStream(
					fileAddedDiff), encoding);

			bigDiffDeletedWriter = new BufferedWriter(osDeletedWriter);
			bigDiffAddedWriter = new BufferedWriter(osAddedWriter);

			for (int i = 0; i <= numOutputFiles; i++) {
				String hexString = Integer.toHexString(i);
				long time = System.currentTimeMillis();
				System.out.println("Sorting and Comparing: " + hexString);
				File prevFile = new File(Settings.outputPath + prevPrefix
						+ hexString + fileExtention);
				File currFile = new File(Settings.outputPath + currPrefix
						+ hexString + fileExtention);
				String[] prevArray = readIntoArray(prevFile);
				String[] currArray = readIntoArray(currFile);
				String[] diffArray = compare(prevArray, currArray);

				for (String line : diffArray) {
					try {
						if (line.startsWith("-")) {
							bigDiffDeletedWriter.append(line.substring(1));
							bigDiffDeletedWriter.newLine();
						} else if (line.startsWith("+")) {
							bigDiffAddedWriter.append(line.substring(1));
							bigDiffAddedWriter.newLine();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				System.out.println("Sorting and Comparing " + hexString
						+ " took "
						+ ((System.currentTimeMillis() - time) / 1000) + "s");

			}

			bigDiffDeletedWriter.flush();
			bigDiffDeletedWriter.close();
			bigDiffAddedWriter.flush();
			bigDiffAddedWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void finalPass() {
		File file = new File(Settings.outputPath + Settings.outputDiffFilename);
		File fileDeletedDiff = new File(Settings.outputPath
				+ Settings.outputDiffDeleted);
		File fileAddedDiff = new File(Settings.outputPath
				+ Settings.outputDiffAdded);

		OutputStreamWriter osWriter = null;
		try {
			osWriter = new OutputStreamWriter(new FileOutputStream(file),
					gzStringEncoding);

			bigDiffResultWriter = new BufferedWriter(osWriter);

			// Get all deleted lines
			System.out.println("Extracting Deleted diffs");
			extractDiffLinesProducerConsumer("-",
					Settings.gzippedTurtleFileOld, fileDeletedDiff);
			System.out.println("Extracting Added diffs");
			extractDiffLinesProducerConsumer("+",
					Settings.gzippedTurtleFileNew, fileAddedDiff);
			System.out.println("Finished extracting Diffs");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (bigDiffResultWriter != null) {
				try {
					bigDiffResultWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/*
	 * Private Functions
	 */

	private void processLine(String line) throws UnsupportedEncodingException {
		byte[] bytesOfLine = line.getBytes(gzStringEncoding);
		md.update(bytesOfLine);
		byte[] encodedLine = md.digest();
		String result = bytesToHex(encodedLine);

		int index = Integer.parseInt(result.substring(0, 2), 16);
		try {
			outputFiles.get(index).append(result);

			outputFiles.get(index).newLine();
		} catch (IOException e) {
			logger.error("Could not add to bucket " + Integer.toString(index),
					e);
		}

	}

	private String[] readIntoArray(File file) {
		LinkedList<String> tempList = new LinkedList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				tempList.add(line);
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
		return tempList.toArray(new String[0]);
	}

	private HashSet<String> readIntoHashSet(File file) {
		LinkedList<String> tempList = new LinkedList<String>();
		BufferedReader reader = null;
		System.out.println("Reading into hash");
		long lineCount = 0;
		long time = System.currentTimeMillis();
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				tempList.add(line);
				lineCount++;
				if (lineCount % 100000 == 0)
					System.out.print(".");
				if (lineCount % 1_000_000 == 0) {
					System.out.println(lineCount / 1_000_000
							+ "m lines read into hashset; "
							+ ((System.currentTimeMillis() - time) / 1000)
							+ "s");
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
		HashSet<String> result = new HashSet<String>(tempList.size() * 2);
		for (String line : tempList) {
			result.add(line);
		}
		return result;
	}

	private String[] compare(String[] prev, String[] curr) {
		// Sort the arrays
		long time = System.currentTimeMillis();
		Arrays.parallelSort(prev);
		Arrays.parallelSort(curr);
		System.out.println("Sort Finished in: "
				+ ((System.currentTimeMillis() - time) / 1000) + "s");
		LinkedList<String> temp = new LinkedList<String>();

		int i = 0, j = 0, max_i = prev.length, max_j = curr.length;

		while (i < max_i || j < max_j) {
			while (i + 1 < max_i && prev[i].equals(prev[i + 1])) {
				i++;
			}
			while (j + 1 < max_j && curr[j].equals(curr[j + 1])) {
				j++;
			}
			if (i >= max_i && j < max_j) {
				temp.add("+" + curr[j++]);
			} else if (j >= max_j && i < max_i) {
				temp.add("-" + prev[i++]);
			} else if (prev[i].equals(curr[j])) {
				i++;
				j++;
			} else if (prev[i].compareTo(curr[j]) < 0) {
				temp.add("-" + prev[i++]);
			} else if (prev[i].compareTo(curr[j]) > 0) {
				temp.add("+" + curr[j++]);
			}
		}
		return temp.toArray(new String[0]);
	}

	private void extractDiffLinesProducerConsumer(final String prefix,
			final String gzFilename, File diffFile) {
		com.axiomzen.util.BlockingQueueBroker<String> broker1 = new com.axiomzen.util.BlockingQueueBroker<String>(
				500);
		final long time = System.currentTimeMillis();
		// final String[] hashedDiffs = readIntoArray(diffFile);
		final HashSet<String> hashedDiffs = readIntoHashSet(diffFile);
		System.out.println("Read into hash");
		count = 0;
		// Create Consumer
		ExecutorService threadPool = Executors.newFixedThreadPool(2);
		threadPool.execute(new com.axiomzen.util.Consumer<String>("", broker1) {

			@Override
			public void consume(String data) {
				if (data != null) {
					try {
						writeDiffLine(data, prefix, hashedDiffs);
						count++;
						if (count % 100000 == 0)
							System.out.print(".");
						if (count % 1_000_000 == 0) {
							System.out.println(count
									/ 1_000_000
									+ "m; "
									+ ((System.currentTimeMillis() - time) / 1000)
									+ "s");
						}
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
			}
		});

		// Create Producer
		java.util.concurrent.Future<?> producerStatus = threadPool
				.submit(new com.axiomzen.util.Producer<String>(broker1) {

					@Override
					public void produce(Brokerable<String> broker)
							throws InterruptedException {

						try {
							logger.trace("Opening zip file");
							is = new GZIPInputStream(new FileInputStream(
									gzFilename));
						} catch (FileNotFoundException e) {
							logger.warn(gzFilename + " not found");
							e.printStackTrace();
							return;
						} catch (IOException e) {
							logger.warn(gzFilename + " IO Error", e);
							e.printStackTrace();
							return;
						}
						logger.trace("Opening buffered reader");
						in = new BufferedReader(new InputStreamReader(is));
						//
						String line = null;
						try {
							logger.trace("Reading Zip File");
							while ((line = in.readLine()) != null) {
								broker.put(line);
							}
						} catch (IOException e) {
							logger.error(e.getLocalizedMessage()
									+ e.getStackTrace());
						} finally {
							try {
								if (in != null)
									in.close();
							} catch (IOException ex) {
								ex.printStackTrace();
							}
						}
					}
				});

		// Wait for Producer to finish
		try {
			logger.trace("Waiting for producer (part 1) to finish");
			producerStatus.get();
		} catch (InterruptedException | ExecutionException e1) {
			e1.printStackTrace();
		}

		logger.trace("Producer finished, shutting down threadPool1");
		threadPool.shutdown();
		try {
			logger.trace("Waiting for termination of threadPool1");
			if (threadPool.awaitTermination(60, TimeUnit.MINUTES)) {
				logger.trace("threadPool1 terminated sucessfully");
			} else {
				logger.warn("threadPool1 did not terminate in time allotted, this could be a problem");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	protected void writeDiffLine(String line, String prefix,
			String[] hashedDiffs) throws UnsupportedEncodingException {
		byte[] bytesOfLine = line.getBytes(gzStringEncoding);
		md.update(bytesOfLine);
		byte[] encodedLine = md.digest();
		String result = bytesToHex(encodedLine);

		try {
			int index = Arrays.binarySearch(hashedDiffs, result);
			if (index >= 0) {
				bigDiffResultWriter.append(prefix + line);
				bigDiffResultWriter.newLine();
			}
		} catch (IOException e) {
			logger.error("Could not add to diff file", e);
		}

	}

	protected void writeDiffLine(String line, String prefix,
			HashSet<String> hashedDiffs) throws UnsupportedEncodingException {
		byte[] bytesOfLine = line.getBytes(gzStringEncoding);
		md.update(bytesOfLine);
		byte[] encodedLine = md.digest();
		String result = bytesToHex(encodedLine);

		try {
			if (hashedDiffs.contains(result)) {
				bigDiffResultWriter.append(prefix + line);
				bigDiffResultWriter.newLine();
			}
		} catch (IOException e) {
			logger.error("Could not add to diff file", e);
		}

	}

	public String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}
}
