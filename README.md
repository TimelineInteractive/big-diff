BigDiff
========

A tool for comparing very large (100GB+), line-oriented data sets.

BigDiff was created to find the differences between similar RDF [N-Triples](http://en.wikipedia.org/wiki/N-Triples) files (for example, consecutive weekly dumps of [Freebase](https://www.freebase.com/)).

However, it should work on any pair of files with the following characteristics:
 - Each line represents a single data record
 - The order of the records has no significance
 - Repetition of records has no significance

If you are concerned only with performance, the C++ version is significantly faster (approximately 40% less execution time on the same input). The Java version is included as a rapid-iteration platform to test new performance ideas.

Both versions of BigDiff expect their input files to be compressed with gzip, and have been tested on several pairs of consecutive [Freebase data dumps](https://developers.google.com/freebase/data). These data files are approximately 26GB compressed, representing 300GB of uncompressed data. BigDiff is able to find all differences using approximately 20GB of virtual memory space. Running on a commodity PC with 16GB of physical RAM, BigDiff can complete its task in less than two hours with minimal swapping.

You can read about the development process in more detail in our blog post, ["Making a Big Difference"](http://axioms.io/zen/2014-06-04-fsk-glass/).

Both versions of BigDiff are ©2014 [Timeline.com, Inc.](http://timeline.com/) and are available under the terms of the attached MIT license. BigDiff was developed in conjunction with [Axiom Zen](https://axiomzen.co).
