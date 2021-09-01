package io.stoys.spark

import io.stoys.scala.Params

import java.time.LocalDateTime

@Params(allowInRootPackage = true)
case class SparkDagRunnerConfig(
    // Main dag class you want to run. Note it has to implement Runnable.
    // The entry point that will be called to construct your graph is function run().
    // IMPORTANT: The constructor has to take Configuration, SparkSession
    //     and SparkDagRunner! Exactly these arguments and in exactly this order!!!
    main_dag_class: String,
    // Timestamp to identify a dag run. It is provided externally.
    run_timestamp: LocalDateTime,
    // List of tables (collections) you want to compute. Use snake cased class names as table names you want to compute.
    // Note: Leaving this empty means nothing will be computed.
    compute_collections: Seq[String],
    // Output path for latest.list (pointer to dag output) and aggregated metrics (using delta table).
    shared_output_path: Option[String],
    // Cache every edge in the dag to avoid computing the same dataset many times.
    disable_caching: Boolean,
    // Debug will force evaluation of dag nodes one by one.
    debug: Boolean
)
