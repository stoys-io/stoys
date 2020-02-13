Spark Notebook Workflow
=======================

What
----

Empower people to write data manipulation (mostly) in SQL in notebook environment.
We believe that is the most productive for data scientists and advanced analytics.
At the same time we want to make it simple to go from notebook to production.
That means tests, git, code review etc.


How
---

1) Create a notebook - `./run.py reporting new --run-id 1234 --notebook-name <awesome>`

2) Do whatever you want / need (in Databricks notebook).

3) You will probably want only some cells to production. Clean their content. Add comment to the first line:

   ```-- REPORT: insights/src/main/scala/com/nuna/trustdb/insights/<awesome>/<awesome>.sql```

   Notes:
     * Obviously use a good names please :)
     * The path under src/main/resources has to be the same as the package of your scala class.

4) Save the notebook - `./run.py reporting save --notebook-name <awesome>`

5) Create scala class, ouptut schema case class, test (and optionally case class for parameters and/or udfs).
   See example at insights/src/main/scala/com/nuna/trustdb/insights/example


Key Ideas
---------

* We don't break notebook workflow (with tables and plots). We just make a few cells production ready
  by adding schema, tests and going through code review and versioning.

* Sql or spark scala api are interchangeable. The function taking number of datasets and returning another dataset
  can be implemented with SQL. But if it is too complicated or too simple then just write it in spark scala api.

* Notebook has actual (SQL) code and some debugging plots and tables. One can explore without bothering about tests.
  One create or modify tests and scala api only before graduating those changes to production.

* Schema - Use case classes. Use Option[*] for nullable fields, java.sql.Date for dates, *_cents: Long for money, etc.

* Granularity - we write tests, schema and documentation for entire insight (or for the data manipulation part
  before ML). Every udf should probably also be tested. In some cases it may be suitable to test smaller part
  of the insight but current Nuna standards are way too fine grained.

* Tests - most tests can be written by cloning canonical record. That means create one record with potentially
  many fields set. But when creating collection for spark test one usually want to change only one or two fields.
  Use myEntity.copy(someField = newValue) method on case class for that. Define createMyEntity (or makeMyEntity)
  if it gets more complicated (like updating some nested entities) where copy method alone does not look readable.


Future Ideas
------------

* Analysts could use latest jar from jenkins and skip the whole java and maven installation and building.
* We can auto generate output case class.
* We can generate the whole stub.
* We can / should kill airflow. Creating a "dag" is nothing more than calling a few scala functions.
* Custom jupyter notebook?
* A lot more ;-)
