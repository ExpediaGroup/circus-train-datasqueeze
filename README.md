<p align="center">
  <img src="CTxDS.png">
</p>

# Circus Train DataSqueeze Copier

## Overview

This project implements new Copiers for [Circus Train](https://github.com/HotelsDotCom/circus-train) which integrate 
[DataSqueeze](https://github.com/ExpediaInceCommercePlatform/datasqueeze).

These CopierFactories are available:

* **DataSqueezeCopier** - Runs DataSqueeze to compact data from source to target (within the same filesystem)
* **S3DataSqueezeCopier** - Composite Copier that first replicates data using the built-in `S3DistCpCopier`, then compacts with `DataSqueezeCopier`

The S3DataSqueezeCopier Composite Copier is built like this:
    
    S3DistCpCopier: Source -> Temp
    CompactionCopier: Temp -> Replica

## Installation

This extension must be added to the Circus Train classpath to be available for use.  These copiers will only be used if specifically requested via the
Circus Train YAML, so adding this extension will not affect other Circus Train jobs accidentally.  

Both Circus Train and DataSqueeze are not bundled with this extension, so they must be installed separately. 

### Classpath

Either copy the JAR from this project into the Circus Train lib folder, or modify `CIRCUS_TRAIN_CLASSPATH` to include them:

    export CIRCUS_TRAIN_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH:/opt/circus-train-datasqueeze/lib/*

## Configuration

Add the following to your Circus Train YAML file:

    ---
      extension-packages: com.expedia.dsp.circustrain
      copier-options:
        copier-factory-class: com.expedia.dsp.circustrain.S3DataSqueezeCopierFactory
        composite-tmp-dir: s3://your-bucket/tmp/composite-tmp-dir
        threshold: 268435456
      table-replications:
        -
          ...

The `composite-tmp-dir` property is used to stage the data between the S3DistCpCopier and the CompactionCopier.

| Property                                | Required | Description
|:----------------------------------------|:--------:|:---
| `extension-packages`                    | Yes      | Allows Circus Train to discover this extension
| `copier-options.copier-factory-class`   | Yes      | Forces Circus Train to use the given copier: `com.expedia.dsp.circustrain.S3DataSqueezeCopierFactory`
| `copier-options.composite-tmp-dir`      | Yes      | The temporary location to stage data between the S3 and DataSqueeze copiers
| `copier-options.threshold`              | No       | A threshold determining which files will be compacted; any files over this size (in bytes) will not be compacted.  If omitted, DataSqueeze will use its [default value](https://github.com/ExpediaInceCommercePlatform/datasqueeze/blob/master/src/main/resources/compaction.properties#L1).

## Contributing

We gladly accept contributions to this project in the form of issues, feature requests, and pull requests!

# Legal

This project is available under the Apache 2.0 License.

Copyright 2018 Expedia Group
