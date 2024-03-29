# Random Sampling for Group-By Queries in Flink
 
![Architecture](https://github.com/vvittis/FlinkSampling/blob/master/Sources/Photos/Architecture.png)
## Purpose
This is a project implenting Random Sampling for Group-By Queries [(1)](https://arxiv.org/pdf/1909.02629.pdf) in Flink Platform.
The goal of this project is to sample streaming tuples, answering effectively Single Aggregate along with a single group-by clause.
This implementation is a two-pass algorithm which is devided into two phases. The first phase has been taken from the first job (pre-processing) and the second phase from the second job (reservoir sampling).

## Pipeline

### Producer
We have implemented a simple Kafka Producer which reads from a csv file (input) and writes to a specified Topic each line of the input file.
The existing implementation does not work for every format of csv...
### Job1
The first job is responsible for the calculation of the mean, the variance and the γi(coefficient of variation-CV),which depends from the previous two variables, for each individual stratum.
In this phase we run across all data of the specified window for the first time.
* It reads all the streaming data from the intercom Topic which has specified by the procuder.
* It sets a timeWindow1( ), whose time is an argument in Job1. 
* It sets a timeWindow2( ), whose time is an argument in Job1. 
* It writes the output data to an intecom Topic (testSink).
#### Window 1
* In this windows we have the KeySelector1( ) which selects the first field (aka group by attributes) and in the process( ) fucntion, calculates the mean and 
the variance based on the generated Iterable 
input and returns a generic Tuple5<String,Double,Double,Double,Integer> **_(T)_**
where the fields are the following:
1. The group bys attributes 
2. The mean
3. The variance
4. The counter of the stratum 
5. The constant value of 1
#### Window 2
* In this window we have the KeySelector2() which selects the last field (aka the constant value of 1) and based on this key the process() function
calculates the γi (variance/mean) of each stratum and finds the total γ which represents the sum of all γi. Lastly, it writes the data in a Sink as mentioned before.
The returned Tuple and the output of the first Job is of the type of **_(T)_** where all fields are the same, except the last one, where now we put the ```si ( M\*(γi/γ),size of stratum )``` of each stratum 
where M is the Memory Budget, which is also an argument for the Job1.

![](Sources/Photos/job1.PNG "Job1")

### Job2 - Sampling

The second job is respondible for sampling the streaming tuples. 
* It reads from two Sources. The first Source (testSource, from Procuder) streams the *datastream* and the second Source (testSink,from Job1) streams the *broadCastStream*
* It broadcasts the *broadCastStream* to all workers and actually sets the terminal conditions to each worker.
* Creates an ArrayList using MapStateDescriptor<String, ArrayList<String>>, esnsuring that each worker has its own ArrayList processed to keep candidate tuples.
* During the connection, in the proces( ) function, it implements reservoir sampling on the streaming tuples and using the *si* and the *counter of stratum* updates the correspoding ArrayList.
* It writes the output data, in the same form as it receives them from Topic testSource, to an output topic (testSink1).

![](Sources/Photos/job2.PNG "Job2")

## Instructions 

In order to run our code.
1. You Download Java Project [Producer](SimpleProducer/SimpleProducer.jar) 
	1. You run as Java Application with inputs
2. You Download Flink Project [Job1](Jars/finaljob1.jar)
3. You open the first Windows Terminal 
	1. Type cd ```C:\flink-1.8.2\bin\``` 
	2. Type C:\flink-1.8.2\bin\start-cluster.bat
	3. ```flink run *"yourJar1Path"* ``` with inputs
4. You Download Flink Project [Job2](Jars/finaljob2.jar)
5. You open the second Windows Terminal
	1. Type ```cd C:\flink-1.8.2\bin\```
	2. ```flink run *"yourJar2Path"*``` with inputs

## Inputs


* **For the Producer**
*"yourPath\population.csv"*


* **For the Job1 the inputs are:**

	```-columns Year,District.Code,District.Name,Neighborhood.Code,Neighborhood.Name,Gender,Age,Number -group_attr District.Name,Year -aggr_attr Number -memory 500 -parallelism 4 -windowTime 60 -windowTime1 20```

* **For the Job2 the inputs are:**

	```-columns Year,District.Code,District.Name,Neighborhood.Code,Neighborhood.Name,Gender,Age,Number -group_attr District.Name,Year -parallelism 4 -output testSink1```
