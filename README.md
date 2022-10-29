# Parquet Reader/Writer Demo

I wanted to learn a bit about reading and writing to [Parquet](https://parquet.apache.org) files. I found
it surprisingly hard as there is a derth of documentation. I finally got a reader and writer running with help from 
[this repository](https://github.com/rdblue/parquet-examples).

This is not very useful or enlightening. It is here more for reference for myself, 
but it may be of some help to anyone who stumbles across it who is trying to do the same thing.

I used [Spring](https://spring.io) in the project, but it is not truly necessary. It just allowed for
some easier configuration of logging and reading of properties files.