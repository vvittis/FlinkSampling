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
* It reads all the streaming data from the intercom Topic which has specified by the procuder.
* It sets a timeWindow1( ), whose time is an argument in Job1. 
* It sets a timeWindow2( ), whose time is an argument in Job1. 
* It writes the output data to an intecom Topic (testSink).
#### Window 1
* In this windows we have the KeySelector1( ) which selects the first field (aka group by attributes) and in the process( ) fucntion, calculates the mean and 
the variance based on the generated Iterable 
input and returns a generic Tuple5<String,Double,Double,Double,Integer> *1*
where the fields are the following:
1. The group bys attributes 
2. The mean
3. The variance
4. The counter of the stratum 
5. The constant value of 1
#### Window 2
* In this window we have the KeySelector2( ) which selects the last field (aka the constant value of 1) and based on this key the process() function
calculates the γi (variance/mean) of each stratum and finds the total γ which represents the sum of all γi. Lastly, it writes the data in a Sink as mentioned before.
The returned Tuple and the output of the first Job is of the type of *1* where all fields are the same, except the last one, where now we put the si (γi/γ) of each stratum.
### Job2

The second job is respondible for 

## Instructions 
## Inputs

## Outline

