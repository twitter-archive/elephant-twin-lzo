## elephant twin lzo

[Elephant Twin](https://github.com/twitter/elephant-twin) is a framework for creating indexes in Hadoop. 

Elephant Twin LZO uses ElephantTwin to create LZO block indexes. The resulting indexes can be used by Elephant Twin's BlockIndexedFileInputFormat to avoid scanning LZO blocks that do not contain the sought values of indexed columns. It can also be used by Elephant Twin's IndexedPigLoader to automatically apply filters found in Pig scripts, reducing the number of scanned data when reading LZO-compressed files.

This project is separate from Elephant Twin due to licensing restrictions due to the GPL.

LZO files are expected to be indexed using the indexing tools found in the [Hadoop-LZO](https://github.com/twitter/hadoop-lzo) package.

## Building

First, ensure that native LZO libraries are installed. 

Use the instructions at https://github.com/twitter/hadoop-lzo for getting this done. 

After that, it should be a regular ```mvn install```.

## Features

```com.twitter.elephanttwin.lzo.retrieval.LZOBlockLevelIndexingJobs``` sets up and runs an indexing job over your lzo files.

## Issues

Have a bug? Please create an issue here on GitHub!

https://github.com/twitter/elephant-twin-lzo/issues

## Versioning

For transparency and insight into our release cycle, releases will be numbered with the follow format:

`<major>.<minor>.<patch>`

And constructed with the following guidelines:

* Breaking backwards compatibility bumps the major
* New additions without breaking backwards compatibility bumps the minor
* Bug fixes and misc changes bump the patch

For more information on semantic versioning, please visit http://semver.org/.

## Authors

Major contributions from 
* Yu Xu
* Jimmy Lin
* Dmitriy Ryaboy

Other contributors can be found via git history (many thanks to all of them).

## License

Copyright 2012 Twitter, Inc.

Licensed under the GNU General Public License. See https://github.com/twitter/elephant-twin-lzo/blob/master/LICENSE
