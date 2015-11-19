---
layout: page
title: "Spark Interpreter Group"
description: ""
group: manual
---
{% include JB/setup %}

## R Interpreter for Zeppelin

### Overview

This is the documentation for the R Interpreter for Zeppelin. 

The R Interpreter presents as two distinct interpreters, `%spark.r%` and `%spark.knitr`. They are alternative interfaces to the same running R session. 

The R Interpreter offers full `SparkR` integration a single Spark and SQL Context that is shared with Python and scala. 

The R Interpreter supports R static image visualizations, and supports through `knitr` several of the most popular interactive visualization packages. 

### Requirements

In addition to the Zeppelin dependencies, the R Interpreter requires the `evaluate` package, which is available from CRAN. 

To use the `%spark.knitr` interpreter, display images through the `%spark.r` interpreter, and so forth, the following optional dependencies can be installed:

 * `knitr`
 * `repr` -- available with `devtools::install_github("IRkernel/repr")`
 * `htmltools`
 * `base64enc`
 
In addition, the R Interpreter supports the following interactive visualization packages: 

 * `googleVis`
 * `rCharts`
 
The R Interpreter does not support `htmlwidgets`, `ggvis`, or `shiny`.  If you are interested in enabling support for these packages, please consider contributing. 

### Installation

To install the R Interpreter add profile r to your build line. 

Such as:

```
mvn package install -Pspark-1.5 -Pr -DskipTests
```

### Enabling SparkR

The R Interpreter will load the `SparkR` package automatically, and use the same Spark Context and SQL Context as the scala and Python interpreters. 

To enable `SparkR`, the SPARK_HOME environment variable must be set when Zeppelin is launched.  

Please do not try to create a Spark Context or SQL Context manually.  If you do, your system will be trying to run multiple Spark Backends in multiple Java VMs simultaneously. 

The variables `sc` and `sql` are automatically created and injected into the R environment. 

### Using the R Interpreter

By default, the R Interpreter appears as two Zeppelin Interpreters, `%spark.r` and `%spark.knitr`. 

%spark.r will behave like an ordinary REPL.  You can execute commands as in the CLI.  Plots will be returned as images embedded in the page, as long as the `repr` and `base64enc` packages are installed. 

If you return a data.frame, Zeppelin will attempt to display it using Zeppelin's visualization tools. 

%spark.knitr interfaces directly against `knitr`, with chunk options on the first line.  Like this:

```
> %sparkr.knitr echo=F,eval=T
hist(rnorm(100))
```

Note that if you use %spark.r and have more than one R expression in a cell, if any of then is a data.frame it will dominate the result and you will see only the Zeppelin visualization of the data.frame, not any results from other expressions. 

The two interpreters share the same environment.  If you define a variable from `%spark.r`, it will be within-scope if you then make a call using `%spark.knitr`.

### Using the Zeppelin Context & Sharing Spark Objects

Because the R Interpreter and the rest of Zeppelin share the same spark and SQL contexts, spark objects created or manipulated from one language are accessible from the other. 

As of this writing, the easiest way to do this is by defining a temp table, and calling the temp table by name from the second language.

An example of this is shown in the RInterpreter example notebook. 

In addition, the Zeppelin Context allows you to move variables from one interpreter to another.  

You can `put` a variable from one language, by name, and `get` it from another language. 

In the R Interpreter, the Zeppelin context can be called with `.z.put(name, object)` and `.z.get(name)`.  

Zeppelin Context support is very beta right now.  It should work for any R variable type that scala can serialize. 
 
R variables brought into scala appear as type `Object` and must be coerced to the appropriate type. 

In addition, it *may* be possible to move RDDs directly between R and scala.  This support is beta, and as of now works only sometimes. 