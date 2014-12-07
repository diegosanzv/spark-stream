spark-stream
============

A very neat mechanism for setting up data flow through a modularized system with Twitter as initial test case. It is meant to be part of a much larger system. However, I only worked on the portion that controlled data flow for aggregation and feature extraction.

The project is written in Scala and particularly shows off its functional features in the context of Spark Streaming API. The system particularly focuses on connecting flows of data through a series of user defined modules. See some of the configuration JSONs to get a better idea of how it all comes together. 

The modules accept inputs and produce output based on their configurations. The inputs are normalized to Entities (essentially free form maps). The flows can then be split or combined using union operators. In the end, the Spark Streaming is configured to build arbitrarily complex transformations of the input streams. This provides an elegant and easy to use interface for building transformation and aggregations and scale.

Demo Videos
===========

[![Showing life trend tracking](http://img.youtube.com/vi/zwlWnX3eQFw/0.jpg)](http://www.youtube.com/watch?v=zwlWnX3eQFw)
