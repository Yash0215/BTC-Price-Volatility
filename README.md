#BTC Volatility Computation: Solution

The solution implements a PySpark application to calculate BTC price volatility for given data. Spark application is implemented to make process fast enough and scalable for large dataset as well. 

####PySpark Application:
The application takes input data as JSON only and apply following transformations:

- Convert field `time_period_start` to date.
- Group data on `date` field to calculate volatility and other usefull stastics on daily basis.
- Write data to Postgres Database for further use.


#### Deployement Design
`spark-submit` script can bes used to run spark application on production with required configuration inside shell scripts. shell scripts are provided for running the job which will be needing packaging before running the spark jobs via `spark-submit`. 

Following are the steps to deploye and run application:

- To build package:
        
        zip -rq src.zip src/

- Provide necessary configuration in `config.ini` file. 
- To run the application:
        
        sh calculate_btc_volatility.sh
 
