package bigdiff4j;

/**
 * Settings file meant for specifying file locations.
 * 
 * This may need to be abstracted out into an input file of some sort
 * 
 * @author kanzhang
 *
 */
public class Settings {
	// Static Strings for paths
	public static final String outputPath = "/media/hdd/db/Timeline/J_Output/";
	public static final String outputPathTest = "/media/hdd/db/Timeline/test/output/";
	public static final String outputDiffDeleted = "bigDiffDeleted.txt";
	public static final String outputDiffAdded = "bigDiffAdded.txt";
	public static final String outputDiffFilename = "bigdiffResult.txt";
	public static final String outputDiffDeletedFilename = "bigdiffDeletedResult.txt";
	public static final String outputDiffAddedFilename = "bigdiffAddedResult.txt";
	public static final String gzippedTurtleDir = "/media/hdd/db/Timeline/freebase_gz/";
	// public static final String gzippedTurtleDir =
	// "/Users/kanzhang/Downloads/";

	// public static final String gzippedTurtleFileOld = gzippedTurtleDir +
	// "freebase-rdf-2014-06-08-00-00.gz";
	// public static final String gzippedTurtleFileNew = gzippedTurtleDir +
	// "freebase-rdf-2014-06-15-00-00.gz";

	public static final String gzippedTurtleFileOld = gzippedTurtleDir
			+ "freebase-rdf-2014-06-08-00-00.gz";
	public static final String gzippedTurtleFileNew = gzippedTurtleDir
			+ "freebase-rdf-2014-06-15-00-00.gz";

}
