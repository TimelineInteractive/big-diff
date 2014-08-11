package differs;

import gnu.trove.set.hash.THashSet;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import utils.P1C1Queue;
import bigdiff4j.MD5Wrapper;
import bigdiff4j.Main;

import com.axiomzen.util.Brokerable;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * DiffLineExtractorBinary
 * 
 * For use with BigDifferBinaryImpl Extracts the lines from the gzip archive
 * that have been marked as different Assumes that bucketize was performed using
 * MurmurHash3 (128 bit) P1C1 Queue adapted from:
 * -http://psy-lob-saw.blogspot.ca
 * /2013/03/single-producerconsumer-lock-free-queue.html
 * 
 * @author kanzhang
 *
 */
public class DiffLineExtractorBinary {

	private static final Logger logger = LogManager.getLogger(Main.class
			.getName());

	// THashSet<MD5Wrapper> hashedDiffs;
	private HashFunction hf;

	private long timeOnHash = 0;
	private long timeOnWrites = 0;

	private int bufferSize = 1048576;

	// Global variables used in an attempt to avoid the garbage collector
	private byte[] encodedLine = new byte[16];
	private MD5Wrapper result = new MD5Wrapper(0L, 0L);

	public DiffLineExtractorBinary() {
		hf = Hashing.murmur3_128();
	}

	// If this function is pass a diff file, then read that diff file into a
	// hash set and call the other function
	public void extractDiffLinesBinaryProducerConsumer(final String gzFilename,
			final BufferedWriter resultWriter, File diffFile) {
		THashSet<MD5Wrapper> hashedDiffs = readIntoByteHashSet(diffFile);
		extractDiffLinesBinaryProducerConsumer(gzFilename, resultWriter,
				hashedDiffs);
	}

	/**
	 * extractDiffLinesBinaryProducerConsumer
	 * 
	 * compare the contents of the gzFilename RDF dump and find the String
	 * representation of all lines in the diffs hash, and write them to the
	 * resultWriter
	 * 
	 * @param gzFilename
	 * @param resultWriter
	 * @param diffs
	 */
	public void extractDiffLinesBinaryProducerConsumer(final String gzFilename,
			final BufferedWriter resultWriter, THashSet<MD5Wrapper> diffs) {

		P1C1Queue<byte[]> broker1 = new P1C1Queue<byte[]>(1_000_000);
		// Timer variable
		final long time = System.currentTimeMillis();

		ExecutorService threadPool = Executors.newFixedThreadPool(2);
		// Create Consumer
		threadPool.execute(new com.axiomzen.util.Consumer<byte[]>("", broker1) {
			@Override
			public void consume(byte[] data) {
				if (data != null) {
					writeDiff(data, resultWriter);
				}
			}
		});

		// Create Producer
		java.util.concurrent.Future<?> producerStatus = threadPool
				.submit(new com.axiomzen.util.Producer<byte[]>(broker1) {
					long count;

					@Override
					public void produce(Brokerable<byte[]> broker)
							throws InterruptedException {
						GZIPInputStream is;
						try {
							logger.trace("Opening zip file");
							is = new GZIPInputStream(new FileInputStream(
									gzFilename), bufferSize);
						} catch (FileNotFoundException e) {
							logger.warn(gzFilename + " not found");
							e.printStackTrace();
							return;
						} catch (IOException e) {
							logger.warn(gzFilename + " IO Error", e);
							e.printStackTrace();
							return;
						}

						// New Byte Method
						byte[] buff = new byte[2 * 256 * 1024];// smaller buffer
						// long count = 0;
						try {
							int ind = 0, from = 0, read;
							// Loop while we can read into the buffer
							while ((read = is
									.read(buff, ind, buff.length - ind)) != -1) {
								// Look for the '\n' character
								for (int i = ind; i < ind + read; i++) {
									if (buff[i] == '\n') {
										// Now that we have found the '\n'
										// character, extract the line up to the
										// '\n'
										byte[] line = new byte[i + 1 - from];
										System.arraycopy(buff, from, line, 0,
												line.length);
										// If this line is a diff line, put into
										// broker
										if (isDiff(line, diffs)) {
											broker.put(line);
										}
										count++;
										if (count % 10_000_000 == 0) {
											System.out.println(count
													/ 1_000_000
													+ "m; "
													+ ((System
															.currentTimeMillis() - time) / 1000)
													+ "s for file "
													+ timeOnHash / 1000
													+ "s on hashing and "
													+ timeOnWrites / 1000
													+ "s on writes");
										}
										from = i + 1;
									}
								}
								System.arraycopy(buff, from, buff, 0,
										buff.length - from);
								ind = ind + read - from;
								from = 0;
								if (read == 0) {
									byte[] tempBuff = new byte[buff.length * 2];
									System.arraycopy(buff, from, tempBuff, 0,
											buff.length);
									buff = tempBuff;
								}
							}

						} catch (IOException e) {
							System.out.println("IOException!");
							e.printStackTrace();
						} finally {
							try {
								is.close();
							} catch (IOException e) {
								logger.warn("Could not close" + gzFilename);
								e.printStackTrace();
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
	}

	/**
	 * isDiff
	 * 
	 * Checks if the input line is a diff
	 * 
	 * @param line
	 *            - byte array representing the line of text from the RDF data
	 *            dump
	 * @param hashedDiffs
	 *            - the hashed diffs
	 * @return boolean representing if this line is a diff
	 * @throws UnsupportedEncodingException
	 */
	protected boolean isDiff(byte[] line, THashSet<MD5Wrapper> hashedDiffs)
			throws UnsupportedEncodingException {
		long hashStart = System.currentTimeMillis();
		// Hash the byte array and store into a global byte array
		hf.hashBytes(line).writeBytesTo(encodedLine, 0, encodedLine.length);

		// fill the global result variable to check with the diff hashes
		this.result.setLeft(fromByteArray(encodedLine, 0));
		this.result.setRight(fromByteArray(encodedLine, 8));
		timeOnHash += System.currentTimeMillis() - hashStart;
		long writeStart = System.currentTimeMillis();

		// Check if this line is a diff
		boolean isDiff = false;
		if (hashedDiffs.contains(result)) {
			isDiff = true;
			hashedDiffs.remove(result);
		}
		timeOnWrites += System.currentTimeMillis() - writeStart;
		return isDiff;
	}

	/**
	 * writeDiff
	 * 
	 * Converts the byte array into a String and writes it to the resultWriter
	 * 
	 * @param line
	 * @param resultWriter
	 */
	protected void writeDiff(byte[] line, BufferedWriter resultWriter) {
		try {
			resultWriter.append(new String(line, BigDiffer.gzStringEncoding));
		} catch (IOException e) {
			logger.warn("Could not write a line!");
			e.printStackTrace();
		}
	}

	/**
	 * readIntoByteHashSet
	 * 
	 * Read the given file into a Trove hash set
	 * 
	 * @param file
	 *            - File to be read
	 * @return - Trove Hash Set
	 */
	private THashSet<MD5Wrapper> readIntoByteHashSet(File file) {

		DataInputStream reader = null;
		System.out.println("Reading into hash");
		long lineCount = 0;
		long time = System.currentTimeMillis();
		try {
			reader = new DataInputStream(new BufferedInputStream(
					new FileInputStream(file)));
			boolean EOF = false;
			while (!EOF) {
				try {
					reader.readLong();
					reader.readLong();
					lineCount++;
					if (lineCount % 10_000_000 == 0) {
						System.out.println(lineCount / 1_000_000
								+ "m lines read into temp list; "
								+ ((System.currentTimeMillis() - time) / 1000)
								+ "s");
					}
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

		// Create Hash Set of Diff lines with previous line count
		System.out.println("Counted a total of " + lineCount + " lines");
		THashSet<MD5Wrapper> result = new THashSet<MD5Wrapper>(
				(int) (lineCount / 0.7));
		lineCount = 0;
		try {
			reader = new DataInputStream(new BufferedInputStream(
					new FileInputStream(file)));
			boolean EOF = false;
			while (!EOF) {
				try {
					result.add(new MD5Wrapper(reader.readLong(), reader
							.readLong()));
					lineCount++;
					if (lineCount % 10_000_000 == 0) {
						System.out.println(lineCount / 1_000_000
								+ "m lines read into hash set; "
								+ ((System.currentTimeMillis() - time) / 1000)
								+ "s");
					}
				} catch (EOFException e) {
					EOF = true;
				}
			}
		} catch (FileNotFoundException e) {
			logger.warn(file.getAbsolutePath() + " not found");
			e.printStackTrace();
		} catch (IOException e) {
			logger.warn(file.getAbsolutePath() + " IO Error");
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

	/*
	 * Helper functions to get a long from a byte array
	 */
	public long fromByteArray(byte[] bytes) {
		return fromByteArray(bytes, 0);
	}

	public long fromByteArray(byte[] bytes, int start) {
		if (bytes.length < start + 8) {
			System.out.println("Byte Array Too Small");
			return 0;
		}
		return fromBytes(bytes[start], bytes[start + 1], bytes[start + 2],
				bytes[start + 3], bytes[start + 4], bytes[start + 5],
				bytes[start + 6], bytes[start + 7]);
	}

	public long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6,
			byte b7, byte b8) {
		return (b1 & 0xFFL) << 56 | (b2 & 0xFFL) << 48 | (b3 & 0xFFL) << 40
				| (b4 & 0xFFL) << 32 | (b5 & 0xFFL) << 24 | (b6 & 0xFFL) << 16
				| (b7 & 0xFFL) << 8 | (b8 & 0xFFL);
	}
}
