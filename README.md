# Random Sampling for Group-By Queries in Flink
 
## Purpose
This is a project implenting Random Sampling for Group-By Queries[(1)](https://arxiv.org/pdf/1909.02629.pdf) in Flink Platform.
The goal of this project is to sample streaming tuples, answering effectively Single Aggregate and Multi Group-Bys queries.
This implementation is a two-pass algorithm which devides into two phases. The first phase has been taken from the first job and the second phase from the second job.


## Pipeline

### Producer
We have implemented a simple Kafka Producer which reads from a csv file (input) and writes to a specified Topic each line of the input file.
The existing implementation does not work for every format of csv...
### Job1
The first job is responsible for the calculation of the mean, the variance and the γi,which depends from the previous two variables, for each individual stratum.
In this phase we run across all data of the specified window for the first time.
⋅⋅* It reads all the streaming data from the intercom Topic which has specified by the procuder.
⋅⋅* It sets a timeWindow1(), whose time is an input to the Job1. 
#### Window 1
⋅⋅* In this windows process() it calculates the mean and the variance based on the generated Iterable input and returns a generic Tuple5<String,Double,Double,Double,Integer> 
where the fields are the following:
1. The group bys attributes 
2. The mean
3. The variance
4. The counter of the stratum 
5. The constant value of 1
#### Window 2
### Job2

## Instructions 
## Inputs

## Outline

