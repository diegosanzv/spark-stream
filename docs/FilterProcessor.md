# Filtering specification
The processor used to perform filtering of the stream. Usually used as a component before other processors. At the moment the thinking is that it will sit in front of the Trending processor.

## Client requirement
The external client will be able to specify the following parameters:

1. Which values to filter on (dot notation)
2. Regular expression to be used as filter

## Stream
This processor implements the Processor trait and modifies the input map.