# Configuration
This document specifies the configuration procedures for ORAC.

The main configuration is represented by a JSON document. See examples in conf/*. There is a loader.py script that reads the configuration JSON and executes the system accordingly. The system is defined in components and the JSON has the following top-level parts:

<pre>
{
    "loader": {
    },
    "system": {
    },
    "sources": [
    ],
    "processors": [
    ],
    "instances": [
    ],
    "flows": [
   ]
}
</pre>

## Top-level parts
* loader: (Object) defines the elements before JVM executes. It accepts 
	* "classpath" array of strings where JARs and paths are defined. 
	* "mainclass" string for the mainclass to execute by the JVM. 
	* "sparkhome" string for the path to the Spark directory.
* system: (Object) defines system properties. The properties defined in this object are global and used for various initialization processes
	* "task_name" name of the job/task
	* "spark_master" URL for spark master, can be local.
	* "spark_home" location of SPARK
	* "stream_duration" duration of a batch window as for defining a spark stream.
	* "jars" array of strings. The list passed to spark for defining a job.
* sources: (Object array) defines the initiators of flows. These are the components that read the specific data and normalize them into DataItems for other processors to work with. For each object on this list a source object is created by the system.
* processors: (Object array) defines the components that process data items. They expect inputs and produce outputs.
* instances: (Object array) defines to the system which processors to instantiate. There can be multiple instances with different parameters of each processor.
* flows: (Object array) defines the flow of data through the system. Each flow must start with a source and continue with processors.

## Source
Sources are components that are data specific. They know how to read from a specialized source (i.e. Kafka, Disk, Twitter, etc) and output DataItems that can be used by processors. Each object in this JSON array represents a source instance. The source classes can be reused, however they must have different IDs. Each object expect the following properties:
* "id" string that identifies this source. This string is used by the flow definition. The string should be unique between instances and sources.
* "classname" string representing the main class of the source object.
* "parameters" an object of key value pairs to be passed as parameters to the source object.
* "data" any JSON object to be passed to the source object.

## Processors
Processors are components that work with potentially multiple inputs of streams. All processors can deal with at least one stream but not all can accept multiple streams. The objects defined in this array, represent a declaration that certain processor exists. It will subsequently need to be instantiated based on the definitions in "instances". The parameters defined in this section serve as defaults for the instances.
* "id" string that identifies this processor. This string is references by the instances.
* "classname" is the string representing the main class for this object.
* "parameters" a map of default parameters. This list will be merged, with override, with the parameters defined in instances 

## Instances
A process instance directs the system to start a processor with specific parameters. Multiple instances of a processor are allowed to exist with different IDs.
* "id" string unique to this instance. This string will be used to define the flows.
* "processId" a reference back to the processors. It takes the id of the processor to instantiate.
* "parameter" map of parameter to be used by the instance. This map has a higher precedence when mapped with default parameters.
* "data" a JSON object to be passed to the instance. It can be any correctly formatted JSON.

## Flows
A flows defines to the system how the processors and streams are interconnected. ID's of processors and stream have to be used here. An array represent a simple branch. However, splits and joins are possible by repeating the names between the flows. For example, flows twitter->p1 and twitter->p2 use the same source but split the stream. In order to join stream the processor at the intersection must be able to support multiple inputs. Each object contains the following properties:
* "id" identifies the flow
* "sequence" an array of strings. The first ID must be a source and later the process instances. The flows define an arbitrary acyclic graph of data flow. Do define two or more paths that contain a common subpath then just repeat the IDs. Those were instances match will be merged to receive multiple inputs or outputs directed to multiple processors.









