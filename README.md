# rZeppelin:  Zeppelin With An R Interpreter 

This is a the Apache (incubating) Zeppelin project, with the addition of support for the R programming language and R-spark integration.

## Why is This a Big Deal for Data Scientists?

Because it allows you to combine R, scala, and Python in a single, seamless, data/machine-learning pipeline, on-the-fly, on ad-hoc clusters, from a simple, integrated, interface. 

Simply put, it can put the power of a Spark cluster into the hands of regular R-using data scientists for their day-to-day work tasks with only a few hours' learning curve.

An ad-hoc cloud cluster can be created in minutes and run for only a few dollars, with no maintenance and no need for assistance from administrators or architects. 

## How Does it Do That?

Zeppelin is a web-based notebook built on top of Spark, kind of like Jupyter.  

Zeppelin has Spark-scala, SQL, and Pyspark interpreters that all share the same Spark context. 

The R Interpreter adds R to the list, again sharing the same Spark context.  

- _Use Case 1_: Given a large amount of text date, you could parse the files from whichever language you choose, then encode it (on the cluster) using Python and `word2vec`, then cluster the embeddings using a scala-based Spark package, and examine the results in R for visualization and to evaluate the clustering quality.  
 
- _Use Case 2_: Let's say that you're working in R on a large text-mining project. You've built your `DocumentTermMatrix` when you realize that training an `LDA` model is going to take a week. In a few minutes you can spin-up a Spark cluster on EC2, write your DTM to the cluster, and train the `LDA` model using Spark's MLlib in an hour instead of a week. Then bring the result right back into R. All for less than the cost of a trip to Starbucks. 

With this package, you can do all of that without switching interfaces, without changing languages, and without having to write intermediate results to disc. 

You can also take full advantage of Spark's lazy execution. 

Together, these capabilities mean that a Spark cluster can be part of an ordinary data scientist's day-to-day toolbox.  

## Zeppelin Documentation 

**Documentation:** [User Guide](http://zeppelin.incubator.apache.org/docs/index.html)<br/>

To know more about Zeppelin, visit [http://zeppelin.incubator.apache.org](http://zeppelin.incubator.apache.org)

## Requirements
 * Java 1.7
 * Tested on Mac OSX, Ubuntu 14.X, CentOS 6.X
 * Maven (if you want to build from the source code)
 * Node.js Package Manager
 * Spark 1.4 or 1.5
 * R 3.1 or later (earlier versions may work, but have not been tested)
 * The `evaluate` R package. 
 
For full R support and to view the demo notebook, you will also need the following R packages:
 
 * `knitr`
 * `repr` -- available with `devtools::install_github("IRkernel/repr")`
 * `htmltools`
 * `base64enc`
 
## Installation & Getting Started

For general instructions on installing Zeppelin, see [https://github.com/apache/incubator-zeppelin](https://github.com/apache/incubator-zeppelin).

To install this build of Zeppelin with the R interpreter, after installing dependencies, execute:

```
git clone -b rinterpreter https://github.com/elbamos/incubator-zeppelin.git
cd incubator-zeppelin
mvn package install -Pspark-1.5 -DskipTests
```

To run Zeppelin with the R Interpreter, zeppelin must be started with the SPARK_HOME environment variable properly set. The best way to do this is from `conf/zeppelin-env.sh`. 

You should also copy `conf/zeppelin-site.xml.template` to `conf/zeppelin-site.xml`.  That will ensure that Zeppelin sees the R Interpreter the first time it starts up. 

To start Zeppelin, from the Zeppelin installation folder:

```
bin/zeppelin.sh # to start zeppelin or
bin/zeppelin-daemon.sh # to start zeppelin as a daemon
```

## Running the Demo Notebook

There is an RInterpreter notebook included.  This is intended for for two groups of people:

* Members of the Zeppelin community who are reviewing the pull request for inclusion, to show them how the R interpreter works. 
* Members of the R community who are not familiar with Zeppelin and may be interested in the R interpreter and Zeppelin for their projects. 

To run the notebook properly the following additional packages should be installed:

 * `devtools`
 * `googleVis`
 * `rCharts`
 
 The notebook is not intended to be part of the submission. 

## Using the R Interpreter

By default, the R Interpreter appears as two Zeppelin Interpreters, `%spark.r` and `%spark.knitr`. 

`%spark.r` will behave like an ordinary REPL.  You can execute commands as in the CLI.  Plots will be returned as images embedded in the page. 

If you return a data.frame, Zeppelin will attempt to display it using Zeppelin's visualization tools. 

`%spark.knitr` interfaces directly against `knitr`, with chunk options on the first line.  Like this:

```
> %sparkr.knitr aknitrchunkname,echo=F,eval=T
hist(rnorm(100))
```

The two interpreters share the same environment.  If you define a variable from `%spark.r`, it will be within-scope if you then make a call using `%spark.knitr`.

**Caveats**:

* The `knitr` environment is persistent. If you run a chunk from Zeppelin that changes a variable, then run the same chunk again, the variable has already been changed.  Use immutable variables. 

* Using the `%spark.r` interpreter, if you return a data.frame, HTML, or an image, it will dominate the result. So if you execute three commands, and one is `hist()`, all you will see is the histogram, not the results of the other commands. This is a Zeppelin limitation.

* If you return a data.frame (for instance, from calling `head()`) from the `%spark.r` interpreter, it will be parsed by Zeppelin's built-in data visualization system. 

## Using SparkR

If SPARK_HOME is set, the SparkR package will be loaded automatically.
 
The Spark Context and SQL Context are created and injected into the local environment automatically as `sc` and `sql`.

That is necessary so that R shares the same Spark and SQL Context with the rest of Zeppelin.

Please, *don't* create your own Spark Context or SQL Context.  You'll end up with multiple jvm's all trying to run Spark backends at the same time. 

## Using the Zeppelin Context to Move Between Languages

Because the R Interpreter and the rest of Zeppelin share the same spark and SQL contexts, spark objects created or manipulated from one language are accessible from the other. 

As of this writing, the easiest way to do this is by defining a temp table, and calling the temp table by name from the second language.

An example of this is shown in the RInterpreter example notebook. 

In addition, the Zeppelin Context allows you to move variables from one interpreter to another.  

You can `put` a variable from one language, by name, and `get` it from another language. 

In the R Interpreter, the Zeppelin context can be called with `.z.put(name, object)` and `.z.get(name)`.  

Zeppelin Context support is very beta right now.  It should work for any R variable type that scala can serialize. 
 
R variables brought into scala appear as type `Object` and must be coerced to the appropriate type. 

In addition, it *may* be possible to move RDDs directly between R and scala.  This support is beta, and as of now works only sometimes. 

## Interactive Visualizations

`knitr` works.  All static visualization packages that I've tested work. 

For interactive visualization, `googleVis` works and `rCharts` works. 

`htmlwidgets` does not work.  Neither does `shiny` or `ggvis.` 

## FAQs

### Why `knitr` Instead of `rmarkdown`?  Why no `htmlwidgets`?

A major difference between `knitr` and `rmarkdown` is that `rmarkdown` uses `pandoc`, an application external to R.

`pandoc` is what does the work of pulling-in dependencies like javascript and css that are needed for `htmlwidgets` and some other visualizations. 

To use `pandoc`, though, `rmarkdown` has to write everything to disc then call `pandoc`, which also writes its output to disc. 

`knitr`, on the other hand, can operate entirely (or almost entirely) in RAM. 

Its the difference between getting a result in a second, and getting a result in a minimum of a minute.  

I would like to support `htmlwidgets` and have done some work on code to handle dependencies, but it is a non-trivial task.

If there is really substantial interest, it isn't *too* challenging to add a `%spark.rmarkdown` interpreter to the mix.

### Why no `ggvis` or `shiny`?

Supporting `shiny` would require integrating a reverse-proxy into Zeppelin, which is something of a task. 

### But I really want to use `ggvis` and `shiny`!!!!

Then help me write the necessary code. This is open source, people.  

### I'm having installation problems with Mac OS X

Are you trying to install on a filesystem that is case-insensitive?  

This is the default for Mac OS X, and almost all Mac root partitions are case insensitive. Apparently this is because Adobe refuses to update Photoshop to run on a case sensitive filesystem.

Whatever.

Anyway, the R Interpreter source lives inside of sub-folder `r/`, but when it compiles, it outputs a compiled R package to sub-folder `R`. 

If your filesystem is case-insensitive, then when you run `mvn clean`, you may end-up inadvertently deleting the `r/` source directory. 

The solution is to install on a case-sensitive file system, don't run `mvn clean` or, if you do, be prepared to re-clone or re-fetch the source. 

### What about Windows?

Zeppelin does not support Windows, therefore the R Interpreter does not support Windows. 

Some early versions of the code attempted to talk to Windows.  No-one managed to get it to work, so I took the support out entirely.

### I'm Getting Weird Errors About the akka Library Version or `TTransport` Errors

You are trying to run Zeppelin with a SPARK_HOME that has a version of Spark other than the one specified with `-Pspark-1.x` when Zeppelin was compiled.

This is an issue with the core Zeppelin build, and not one I can fix.  The only option is to recompile. 

### The R Interpreter Does Make a Spark Context and it Says It Can't Find SparkR, but Everything Else Works

You don't have SPARK_HOME set properly, or you have the spark.home Zeppelin configuration variable set. 

Because the version of `SparkR` has to match the version of Spark, the R Interpreter only looks for `SparkR` under SPARK_HOME. 

If it can't find `SparkR` there, unless there is a `SparkR` in the R library path, the R Interpreter will not be able to connect to the Spark Context. 

### But Everything Else Works So Its All Your Fault!

Like I said, the Zeppelin-Spark interface has changed in major ways repeatedly, and this has repeatedly broken the R Interpreter. 

I no longer attempt to support all of the Spark configuration options supported by Zeppelin for this reason. 

I support only one, which is running with SPARK_HOME set, and spark.home unset. 

### Will This Fork Track Zeppelin-Master?

No. At least, I won't promise that it will. 

I intend to track all Zeppelin releases.  And I will periodically rebase Zeppelin-Master into the code.  But I won't promise to track Zeppelin-Master consistently. 

### Why Fork Zeppelin?

The R Interpreter continues to be a pending pull request that is under discussion within the Zeppelin community. 

I hope that it will be acted-upon soon.  