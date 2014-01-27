# Trending vision
## Ovierview
Clients will be able to use a trending service in a number of modes: 

* As part of a route where data is streamed into a trending component. In this mode the current time is used to provide the time dimension.
* As a stand alone service. This is similar to above but no other processing is being performed.
* As part of a batch process. In this mode, either a field will be provided, or some part of the filename will be used to provide the time dimension.

In each case there will be additional algorithm parameters provided by the client.

## Client requirement
The external client will be able to specify the following parameters:

1. Algorithm parameters
	* Which algorithm to use
	* The size of the window (in seconds)
	* The emit frequency is set by the batch size of the DStream.

2. Input parameters
	* Where in the input map to read the data from. Dot notation for the property name.
	* duration: The window to compute the summary on. In Seconds
	* slide_duration: By how many seconds to slide the window. Must be a multiple of the parent stream sample window.

3. Output parameters
    * approximation error rate - algorithm specific value.
	* Where to put the data (trend_file parameter in the TrendingStorage processor).
	* How many top results to emit

## Component requirement
The trending component will have parameters as follows:

1. The client configuration specified above.
2. Storage mechanism to use with associated configuration:
 	*	Method to use (Redis, File)
3.  Algorithm parameters
	* Method to use (Lossy Counting, Linear Propabilistic Counting, Lossless Counting)

## Storing output
We will want to define and change storage mechanisms for trending. Example output mechanisms are:

1. Redis storage
2. File (including HDFS) storage
3. Kafka queue
 
In order to separate the trending algorithm from storage there will be a required interface that. This will be runtime specified using the components configuration.

## Component Usage
The trending component will need to work in both batch data and streamed data. This may be done via separate external methods calls and internals. This should be implenented via the framework as a simulated flow (i.e. replay). A parameter will be passed in case the components needs to act differently to support this.

## Component Implementation
The trending component will conform to the Spark Processor definition, that is:

1. It will have a constructor that takes a `ProcessConfig` object. This will define all the required parameters within its `parameters: Map[String, String]` field.
2. It will process the data by implementing the `def process(input: Map[String, Any]): Map[String, Any]` method (as defined on the `Processor` trait.
3. It will do no harm to the input map. That is it will pass it on unchanged (it is most likely the output will be the input). 

