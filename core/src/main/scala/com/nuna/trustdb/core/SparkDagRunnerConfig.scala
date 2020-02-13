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
    // timestamp to identify a dag run. It is provided externally
    runTimestamp: LocalDateTime,
    // List of tables (collections) you want to compute. Put here the names
    // (snake cased class names) of tables you want to compute.
    // Note: Leaving this empty means nothing will be computed.
    computeCollections: Seq[String],
    // Debug will force evaluation of dag nodes one by one.
    debug: Boolean
)
