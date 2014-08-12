package differs;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import utils.P1C1Queue;
import bigdiff4j.Main;

import com.axiomzen.util.Brokerable;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * BucketizerBinary
 * 
 * For use with BigDifferBinaryImpl Reads a gzipped archive and hashes each
 * line, then writes it to different buckets (files) using the first byte as the
 * index of the file to write to Currently using MurmurHash3 (128 bit) P1C1
 * Queue adapted from:
 * -http://psy-lob-saw.blogspot.ca/2013/03/single-producerconsumer
 * -lock-free-queue.html
 *
 * @author kanzhang
 *
 */
public class BucketizerBinary {

	private static final Logger logger = LogManager.getLogger(Main.class
			.getName());

	private HashFunction hf;
	private ArrayList<DataOutputStream> outputFiles = new ArrayList<DataOutputStream>(
			BigDiffer.numOutputFiles);

	// From NIO Test, Unsuccessful.
	// private ArrayList<FileChannel> outputChannels = new
	// ArrayList<FileChannel>(BigDiffer.numOutputFiles);

	private long timeOnHash = 0;
	private long timeOnWrites = 0;

	private int bufferSize = 1048576;

	private static byte[] encodedLine;
	// private ByteBuffer writeBuff;

	private String outpath;
	private String prefix;

	public BucketizerBinary(String out, String prefix) {
		outpath = out;
		this.prefix = prefix;
		hf = Hashing.murmur3_128();
		initializeBucketWriters(prefix);
	}

	// Shortcut for running the bucketizer right when you create it. Not
	// currently used
	public BucketizerBinary(String out, String prefix, String gzFilename) {
		outpath = out;
		this.prefix = prefix;
		hf = Hashing.murmur3_128();
		initializeBucketWriters(prefix);
		bucketize(gzFilename);
	}

	// Initializes all the bucket writers, will overwrite any files of the same
	// name that currently exist
	public void initializeBucketWriters(String prefix) {

		for (int i = 0; i <= BigDiffer.numOutputFiles; i++) {
			String hexString = Integer.toHexString(i);
			File file = new File(outpath + prefix + hexString
					+ BigDiffer.fileExtention);
			DataOutputStream dataStream = null;

			try {
				dataStream = new DataOutputStream(new BufferedOutputStream(
						new FileOutputStream(file)));
			} catch (IOException e) {
				logger.warn(file.getAbsolutePath() + " IO Error", e);
				e.printStackTrace();
			}

			outputFiles.add(i, dataStream);
		}

		/*
		 * // Used for NIO without memory mapped nio 
		 * writeBuff = ByteBuffer.allocate(16); 
		 * for (int i =0; i<=BigDiffer.numOutputFiles; i++) { 
		 * String hexString = Integer.toHexString(i); 
		 * File file = new File(outpath + prefix + hexString + BigDiffer.fileExtention);
		 * FileChannel dataChannel = null;
		 * 
		 * try { 
		 * dataChannel = new FileOutputStream(file).getChannel(); 
		 * } catch (IOException e) { 
		 * e.printStackTrace();
		 * }
		 * 
		 * outputChannels.add(i, dataChannel); }
		 */

		// Registers a shutdown hook for the buffered writters instance so that
		// it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (DataOutputStream writer : outputFiles) {
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

		timeOnHash = 0;

	}

	// Reads from the proved gzip and writes to the buckets that should have
	// been initialized by initializeBucketWriters function
	public void bucketize(String gzFilename) {
		// Initialize the lock-free Single Producer Single Consumer Queue
		// (Capacity 1 Million)
		P1C1Queue<byte[]> broker1 = new P1C1Queue<byte[]>(1_000_000);

		// Timer variable
		long time = System.currentTimeMillis();

		// Create Consumer
		ExecutorService threadPool = Executors.newFixedThreadPool(2);
		threadPool.execute(new com.axiomzen.util.Consumer<byte[]>("", broker1) {
			long count = 0;

			@Override
			public void consume(byte[] data) {
				if (data != null) {
					try {
						processLineBinary(data);
						count++;
						if (count % 10_000_000 == 0) {
							System.out.println(count
									/ 1_000_000
									+ "m; "
									+ ((System.currentTimeMillis() - time) / 1000)
									+ "s for " + prefix + "XX.txt files with "
									+ timeOnHash / 1000 + "s on hashing and "
									+ timeOnWrites / 1000 + "s on writes");
						}
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
			}
		});

		// Create Producer
		java.util.concurrent.Future<?> producerStatus = threadPool
				.submit(new com.axiomzen.util.Producer<byte[]>(broker1) {
					GZIPInputStream is;

					@Override
					public void produce(Brokerable<byte[]> broker)
							throws InterruptedException {

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
						byte[] buff = new byte[8 * 256 * 1024];// smaller buffer
						try {
							int ind = 0, from = 0, read;
							while ((read = is
									.read(buff, ind, buff.length - ind)) != -1) {
								for (int i = ind; i < ind + read; i++) {
									if (buff[i] == '\n') {
										byte[] line = new byte[i + 1 - from];
										System.arraycopy(buff, from, line, 0,
												line.length);
										broker.put(line);
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
								logger.warn("Could not close " + gzFilename);
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

		// Close all the bucket writers
		closeOutputFiles();

	}

	private void closeOutputFiles() {
		for (DataOutputStream writer : outputFiles) {
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

	/**
	 * processLineBinary
	 * 
	 * The function used to hash the lines from the RDF and save them as binary
	 * into the correct file
	 * 
	 * @param line
	 * @throws UnsupportedEncodingException
	 */
	private void processLineBinary(byte[] line)
			throws UnsupportedEncodingException {
		long hashStart = System.currentTimeMillis();
		// Hash the lines
		encodedLine = hf.hashBytes(line).asBytes();
		timeOnHash += System.currentTimeMillis() - hashStart;
		long writeStart = System.currentTimeMillis();

		try {
			// Write the hashed lines into the corresponding output file
			outputFiles.get(encodedLine[0] & 0x000000ff).write(encodedLine);
		} catch (IOException e) {
			logger.fatal(
					"Could not add to bucket "
							+ Integer.toString(encodedLine[0]), e);
		}
		timeOnWrites += System.currentTimeMillis() - writeStart;
	}
}
