spark-stream
============

A very neat mechanism for setting up data flow through a modularized system with Twitter as initial test case. It is meant to be part of a much larger system. However, I only worked on the portion that controlled data flow for aggregation and feature extraction.

The project is written in Scala and particularly shows off its functional features in the context of Spark Streaming API. The system particularly focuses on connecting flows of data through a series of user defined modules. See some of the configuration JSONs to get a better idea of how it all comes together. 

The modules accept inputs and produce output based on their configurations. The inputs are normalized to Entities (essentially free form maps). The flows can then be split or combined using union operators. In the end, the Spark Streaming is configured to build arbitrarily complex transformations of the input streams. This provides an elegant and easy to use interface for building transformation and aggregations and scale.

Demo Videos
===========
The first video shows how the tool receives data from twitter, parses it using the modular pipeline and outputs hashtag trending information in near realtime. This was the first iteration of the system.

[![Showing live trend tracking](http://img.youtube.com/vi/zwlWnX3eQFw/0.jpg)](http://www.youtube.com/watch?v=zwlWnX3eQFw)

On the next iteration I was able to set up a much more complex data flow. This video shows it can be set up. Notice the programatic style in how the modules can build up complex logic from simple building blocks. This file can be found in example configuration along with other setups.

[![Showing configuration of a complex dataflow](http://img.youtube.com/vi/wXNv9BZxzMY/0.jpg)](http://www.youtube.com/watch?v=wXNv9BZxzMY)
