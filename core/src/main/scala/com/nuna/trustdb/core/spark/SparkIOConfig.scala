package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.util.Params

@Params(allowInRootPackage = true)
case class SparkIOConfig(
    inputPaths: Seq[String],
    outputPath: String,
    // If provided, publish two artifacts: 1) a list of input and output tables 2) aggregated metrics (using delta).
    // NOTE: there is no use case yet which requires two different paths for each artifact
    sharedOutputPath: Option[String],
    writeFormat: String,
    writeMode: String,
    writeOptions: Map[String, String]
)
