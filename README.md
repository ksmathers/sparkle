# Introduction

Once upon a time there was a data engineer who had to do all of his development work using a Web IDE.  Every build 
he would sit and stare at the screen for five or ten minutes waiting for the build to be checked, for a machine cluster to be allocated, and for the build to run, all while his high end development workstation stood idle.  Twenty-four cores and thirty-two gigs of memory all resting comatose because all computation work could only be done on the cloud.

Foundry isn't a bad development environment, but even if you stick to the 'Preview' calculations or run most of your analyses in workbooks so that you don't have to wait through a pipeline build, there isn't a good UI for playing with code in development.  

# What is Sparkle

The Sparkle library implements parts of the Foundry API so that you can simply copy and paste code from Foundry transforms into a Jupyter notebook to play with intermediate results and inspect spark dataframe contents and partial results interactively.   Think of Sparkle as a slightly more flexible version of workbooks, with the added benefit of being able to dump the data into matplotlib or seaborne for visualization without the restrictions or mouse-click dependent work of setting up a visualization in workbooks.

# Sparkle API

## SparkleRuntime()

Container for a Spark runtime and a set of transforms to run in a batch.   This is analogous to a Foundry build but 
doesn't solve the dependency graph, instead you just add the transforms that you want to run and then submit and the transforms will run in the order requested.

add_transform(tf : Transform)

> Adds a transform to the build

submit()

> Runs the previously added transforms.   Submit will also start the spark session if it hasn't already been started.

## Transform

Transforms are created by use of a decorator attached to a transformation function. 

```
@transform_df(output : Output, input1 : Input, ...)
def mytransform(ctx, input1, ...):
    result = input1
    return result
```

Transformation functions are called with the spark context as the first parameter, followed by each of the 
inputs as defined in the decorator which are delivered to the function as Spark DataFrames.   Inputs are loaded before your function is invoked and the Dataframe that you return as a result will be written by the containing
environment.

In Foundry including the 'ctx' parameter is optional and is not populated if it is not present in the function
signature.  In Sparkle v0.01 the 'ctx' parameter is required.

## Input

Input(path)

> Inputs define a data source to use for reading a Spark dataframe.   Currently the supplied path must point to a CSV file, or a directory that contains a set of related CSV files as RDD segments.

## Output

Output(path)

> Outputs define a data sink to use for writing a Spark dataframe.   Currently the supplied path must point to a directory where CSV files will be written as RDD segments.


# Examples

The following example demonstrates how to use the initial release of the Sparkle library.   Code before and 
after the '-- snip --' comments are boilerplate code specific to setting up the Sparkle library and unrelated
to any APIs found in Foundry.   The code between the two '-- snip --' comments should be runnable in Foundry
provided that the referenced file paths are replaced with FoundryFS paths to which the user has access.

```python
from sparkle import SparkleRuntime, Input, Output, transform_df

rt = SparkleRuntime()
rt.start()

# -- snip --

@transform_df(
    Output("testdata/result2"),
    xx=Input("testdata/oscar_age_female.csv"),
    xy=Input("testdata/oscar_age_male.csv")
)
def mytransform(ctx, xx : DataFrame, xy : DataFrame):
    return (xx.withColumn('Gender', F.lit('female'))
            .union(xy.withColumn('Gender', F.lit('male')))
            .orderBy(xx.Age))

# -- snip --

rt.transforms.clear()
rt.add_transform(mytransform)
rt.submit()
```