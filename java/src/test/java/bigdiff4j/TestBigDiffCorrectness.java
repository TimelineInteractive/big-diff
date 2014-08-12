package bigdiff4j;

import static org.junit.Assert.*;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import differs.BigDiffer;
import differs.BigDifferBinaryImpl;

public class TestBigDiffCorrectness {
	private BufferedReader in;
	private GZIPInputStream is;

	private ArrayList<DataOutputStream> outputFiles;
	int bothSize = 100_000;
	int diffSize = 10_000;
	HashSet<String> both;
	HashSet<String> different;

	String smallerGZFilePath = Settings.outputPathTest + "smaller.gz";
	String largerGZFilePath = Settings.outputPathTest + "larger.gz";

	@Before
	public void setUp(){
		both = new HashSet<String>((int) (bothSize/0.75));
		different = new HashSet<String>((int) (diffSize/0.75));


		try {
			is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFileOld));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		in = new BufferedReader(new InputStreamReader(is));
		//
		String line = null;
		try {
			while((line = in.readLine()) != null && both.size()<bothSize) {
				both.add(line);
			}
			while((line = in.readLine()) != null && different.size()<diffSize) {
				if (!both.contains(line)){
					different.add(line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (in != null) in.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		FileOutputStream output = null;
		try {
			output = new FileOutputStream(smallerGZFilePath);
			Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
			try {
				for (String text : both){
					writer.write(text+"\n");
				}
			} finally {
				writer.close();
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			output = new FileOutputStream(largerGZFilePath);
			Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
			try {
				for (String text : both){
					writer.write(text+"\n");
				}
				for (String text : different){
					writer.write(text+"\n");
				}
			} finally {
				writer.close();
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}


	}
	@After
	public void tearDown(){

	}
	@Test
	public void testDeletedItems() {
		// Read 100,000 lines from the rdf, Hash and store as "old "
		// Read an additional 10,000 lines, and ensure that they are not in the past 100,000 then hash and store as "diff"
		// Add the 10,000 lines to the 100,000 lines from before ("new") and store these 110,000 lines as "old"
		// Run big diff, and then compare against the 10,000 lines in "diff"


		assertEquals(both.size(), bothSize);
		assertEquals(different.size(), diffSize);



		BigDifferBinaryImpl differ = new BigDifferBinaryImpl();

		differ.setOutputPath(Settings.outputPathTest);

		// Bucketize
		differ.bucketize(BigDiffer.prevFilePrefix, largerGZFilePath);
		differ.bucketize(BigDiffer.currFilePrefix, smallerGZFilePath);

		// Sort and Compare
		differ.bigDiff(BigDiffer.prevFilePrefix, BigDiffer.currFilePrefix);

		// Diff Extraction
		differ.finalPass(largerGZFilePath,smallerGZFilePath);

		// Read in the diff for comparison
		File file = new File(Settings.outputPathTest + Settings.outputDiffDeletedFilename);
		LinkedList<String> result = new LinkedList<String>();
		long lineCount = 0;
		long time = System.currentTimeMillis();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				result.add(line);
				lineCount++;
				if (lineCount % 100000 == 0) System.out.print(".");
				if (lineCount % 1_000_000 == 0) {
					System.out.println(lineCount/1_000_000 + "m lines read into hashset; " + ((System.currentTimeMillis()-time)/1000) + "s");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Compare the Diffs
		assertEquals(different.size(), result.size());
		boolean pass = true;
		for (String resultLine : result){

			if (!different.contains(resultLine)){
				pass = false;
				System.out.println("NOT IN HASH MAP:" + resultLine);
			}
		}
		assertTrue(pass);


	}


	@Test
	public void testAddedItems() {
		// Read 100,000 lines from the rdf, Hash and store as "new "
		// Read an additional 10,000 lines, and ensure that they are not in the past 100,000 then hash and store as "diff"
		// Add the 10,000 lines to the 100,000 lines from before ("old") and store these 110,000 lines as "old"
		// Run big diff, and then compare against the 10,000 lines in "diff"
		assertEquals(both.size(), bothSize);
		assertEquals(different.size(), diffSize);



		BigDifferBinaryImpl differ = new BigDifferBinaryImpl();

		differ.setOutputPath(Settings.outputPathTest);

		// Bucketize
		differ.bucketize(BigDiffer.prevFilePrefix, smallerGZFilePath);
		differ.bucketize(BigDiffer.currFilePrefix, largerGZFilePath);

		// Sort and Compare
		differ.bigDiff(BigDiffer.prevFilePrefix, BigDiffer.currFilePrefix);

		// Diff Extraction
		differ.finalPass(smallerGZFilePath,largerGZFilePath);

		// Read in the diff for comparison
		File file = new File(Settings.outputPathTest + Settings.outputDiffAddedFilename);
		LinkedList<String> result = new LinkedList<String>();
		long lineCount = 0;
		long time = System.currentTimeMillis();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				result.add(line);
				lineCount++;
				if (lineCount % 100000 == 0) System.out.print(".");
				if (lineCount % 1_000_000 == 0) {
					System.out.println(lineCount/1_000_000 + "m lines read into hashset; " + ((System.currentTimeMillis()-time)/1000) + "s");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Compare the Diffs
		assertEquals(different.size(), result.size());
		boolean pass = true;
		for (String resultLine : result){

			if (!different.contains(resultLine)){
				pass = false;
			}
		}
		assertTrue(pass);
	}

	@Test
	public void testIgnoreDuplicates() {
		// Read 100,000 lines from the rdf, Hash and store as "new "
		// Read an additional 10,000 lines, and ensure that they are not in the past 100,000 then hash and store as "diff"
		// Add the 10,000 lines to the 100,000 lines from before ("old") and store these 110,000 lines as "old"
		// Add the same 10,000 lines again to the 110,000 lines
		// Run big diff, and then compare against the 10,000 lines in "diff", make sure that only the 10,000 lines show up, and no more
		assertEquals(both.size(), bothSize);
		assertEquals(different.size(), diffSize);
		FileOutputStream output = null;
		try {
			output = new FileOutputStream(largerGZFilePath);
			Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
			try {
				for (String text : both){
					writer.write(text+"\n");
				}
				for (String text : different){
					writer.write(text+"\n");
				}
				for (String text : different){
					writer.write(text+"\n");
				}
			} finally {
				writer.close();
			}
		}  catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		BigDifferBinaryImpl differ = new BigDifferBinaryImpl();

		differ.setOutputPath(Settings.outputPathTest);

		// Bucketize
		differ.bucketize(BigDiffer.prevFilePrefix, smallerGZFilePath);
		differ.bucketize(BigDiffer.currFilePrefix, largerGZFilePath);

		// Sort and Compare
		differ.bigDiff(BigDiffer.prevFilePrefix, BigDiffer.currFilePrefix);

		// Diff Extraction
		differ.finalPass(smallerGZFilePath,largerGZFilePath);

		// Read in the diff for comparison
		File file = new File(Settings.outputPathTest + Settings.outputDiffAddedFilename);
		LinkedList<String> result = new LinkedList<String>();
		long lineCount = 0;
		long time = System.currentTimeMillis();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				result.add(line);
				lineCount++;
				if (lineCount % 100000 == 0) System.out.print(".");
				if (lineCount % 1_000_000 == 0) {
					System.out.println(lineCount/1_000_000 + "m lines read into hashset; " + ((System.currentTimeMillis()-time)/1000) + "s");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Compare the Diffs
		assertEquals(different.size(), result.size());
		boolean pass = true;
		for (String resultLine : result){

			if (!different.contains(resultLine)){
				pass = false;
			}
		}
		assertTrue(pass);
	}



	public void initializeBucketWriters(String prefix) {
		outputFiles = new ArrayList<DataOutputStream>(BigDiffer.numOutputFiles);
		for (int i =0; i<=BigDiffer.numOutputFiles; i++){
			String hexString = Integer.toHexString(i);
			File file = new File(Settings.outputPathTest + prefix + hexString + BigDiffer.fileExtention);
			DataOutputStream dataStream = null;

			try {
				dataStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			} catch (IOException e) {
				e.printStackTrace();
			}

			outputFiles.add(i, dataStream);
		}

		// Registers a shutdown hook for the buffered writters instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread() {
			@Override
			public void run() {
				for (DataOutputStream writer : outputFiles) {
					if(writer != null) {
						try {
							writer.flush();
							writer.close();
						} catch (IOException e) {
							e.printStackTrace();
						}

					}
				}
			}
		} );
	}


}
