# Datalake-Light ETL

An ETL framework for spark-based Data Lake applications.
The goal of the framework is to enable building spark applications with as little effort as possible.

For small data, using spark can be overkill, so support for plain text (e.g., csv) files was added.

This framework is to be tied together with a workflow scheduling and an infrastructure framework (both WIP).

## Roadmap

* Create Azure Devops ci pipeline for publishing
* Build higher level components, like jobs
* Add pandas dataframe support

## Developer notes

* Install python dependencies with `make install`
* Run tests with `make test`
* Check code with `make code`
* Build with `make buid`