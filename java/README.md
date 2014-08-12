bigdiff4j
=========

*Function*

Takes two gzip RDF files from freebase and using a disk-based sorting algorithm, finds the differences between the two.

*Prerequisits:*

- Java 8 JDK (http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- Eclipse w/ Java 8 Compatibility (Can be Found on Eclipse Market Place)
- m2e Maven eclipse plugin (Can be Found on Eclipse Market Place)

*Steps to get running:*

- Clone repo
- Open eclipse using the root of the project as the workspace.
- Select 'import' existing maven project from directory (or something like that)
- Probably need to set the correct JDK in the build path for each Project (J2SE JavaSE 1.7)
- If eclipse complains that JUnit is missing, add JUnit4 in the project properties->Java Build Path-> Libraries
- need "Utilities" project from https://github.com/axiomzen/timeline-java.git (clone if necessary)
- Import Utilities as a maven project
- Project -> Properties -> Java Build Path -> Projects -> Add... -> Utilities
- Set up Settings and Run Configurations (See Below)

*Run Configuration*

- Highly recommend using -XX:+UseConcMarkSweepGC
- 32 gb ram also highly recommended. Minimum 16 gb ram. -Xmx26g for 32gb ram systems or -Xmx14g for 16gb ram systems

*Settings*

- Must modify Settings Class to point to proper location of the two RDFs, and the desired output location.
- If you are planning to run the unit-test as well, ensure that your `outputPathTest` is also set

*Tests*
Currently tests correctness by using a small subset of RDFs, (100,000), as well as a similar set of 110,000 (100,000 same as before plus 10,000 new rdf lines), and sees if it picks out all 10,000 differences
Also checks for functioning duplicate elimination
