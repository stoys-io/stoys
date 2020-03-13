package com.nuna.trustdb.core

import java.time.LocalDateTime

import com.nuna.trustdb.core.util.Params

@Params(allowInRootPackage = true)
case class SparkDagRunnerConfig(
    // Main dag class you want to run. Note it has to implement Runnable.
    // The entry point that will be called to construct your graph is function run().
    // IMPORTANT: The constructor has to take Configuration, SparkSession
    //     and SparkDagRunner! Exactly these arguments and in exactly this order!!!
    mainDagClass: String,
    // Timestamp to identify a dag run. It is provided externally.
    runTimestamp: LocalDateTime,
    // List of tables (collections) you want to compute. Use snake cased class names as table names you want to compute.
    // Note: Leaving this empty means nothing will be computed.
    computeCollections: Seq[String],
    // Output path for latest.list (pointer to dag output) and aggregated metrics (using delta table).
    sharedOutputPath: Option[String],
    // Cache every edge in the dag to avoid computing the same dataset many times.
    disableCaching: Boolean,
    // Debug will force evaluation of dag nodes one by one.
    debug: Boolean
)
