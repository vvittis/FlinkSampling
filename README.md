# Random Sampling for Group-By Queries in Flink
 
## Purpose
This is a project implenting Random Sampling for Group-By Queries[(1)](https://arxiv.org/pdf/1909.02629.pdf) in Flink Platform.
The goal of this project is to sample streaming tuples, answering effectively Single Aggregate and Multi Group-Bys queries.


## Pipeline

### Producer
	⋅⋅⋅We have implemented a simple Kafka Producer which reads from a csv file (input) and writes to a specified Topic each line of the input file.
### Job1
	⋅⋅⋅The first job is responsible to calculate the mean, the variance and the γi for each individual group by.
### Job2

## Instructions 
## Inputs

## Outline

