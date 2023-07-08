# SKEWHJs
Hello, here is the project code A of the paper "One Size Cannot Fit All: a Self-Adaptive Solution towards Skewed Hash Join in Shared-nothing RDBMSs". We use cockroach(v20.1.174) as the prototype code to implement the entire solution mentioned in the paper. The changed files in the project have been organize to this repository. To run this project, just replace them with the corresponding files in the cockroach.

## data.proto
```
replace position: pkg/sql/execinfrapb/data.proto
```
We added the distribution rule of MixHashRouter and the definition of redistribution method type to this file.

## processors_base.proto
```
replace position: pkg/sql/execinfrapb/processors_base.proto
```
Added some definitions.

## distsql_physical_planner.go
```
replace position: pkg/sql/distsql_physical_planner.go
```
We added the CBD(cost-based dispatcher) code to the physical plan generation part of the connection operator. After the CBD decision, the working parameters of the hash connection are passed to the output part of the Tablereader operator.

## distsql_plan_join_opt.go
```
replace position: pkg/sql/distsql_plan_join_opt.go
```
This file is the core code of CBD, which contains the definition of the cost model interface and three algorithms to implement the interface. Each algorithm is abstracted into a class, which respectively contains the definition of the cost calculation method in three stages and the working parameters of the algorithm. If you need to add candidate algorithms, you only need to implement the cost model interface. Three cluster setting switches are defined in the file to control the execution algorithm of distributed hash connection.

## routers.go
```
replace position: pkg/sql/rowflow/routers.go
```
We have implemented a new Router class to implement PnR, PRPD and GraHJ.

## physical_plan.go
```
replace position: pkg/sql/physicalplan/physical_plan.go
```
We added the definition of hash connection working parameters and the implementation of AddJoinStageWithWorkArgs() method.

## join.go
```
replace position: pkg/sql/join.go
```
Added some definitions.

## distsql_plan_join.go
```
replace position: pkg/sql/distsql_plan_join.go
```
Added some definitions.
# Datasets
We use the Python numpy package to generate datasets that follow the Zipf distribution. The code for generating the dataset is in the zipf folder and can be used in the following way.
```
cd zipf
python main.py + argvs
```
Parameters are as follows.
```
Parameters
{
    argv[1] : num_row_1
    argv[2] : a_1
    argv[3] : num_row_2 
    args[4] : a_2
    args[5] : upper_bound
    args[6] : offset(optional, by default 0)
}
Data scope: [0, upper_bound]
```
The five parameters are the number of rows to generate the small table, the zipf factor of the small table (a decimal number between 1.01 and 2), the number of rows of the large table, the zipf factor of the large table, and the upper bound of the data (the large table and the small table have a data range of [0,upper_bound] on the join column).
# Workload
The workload of the experiment is a query in the following form. R table and S table perform an equi-join on a column.
```
SELECT COUNT ( ∗ ) FROM R, S WHERE R.a=S.b ;
```
# Evaluation
## Experimental Setup
```
CPU: Intel Xeon Gold 6240 ×2, a total of 36 cores
Memory: 64GB
Persistent storage: SSD
Network card: Mellanox
MT27710 25Gbps
```
```
Operating system: Ubuntu 18.04
Database: CRDB v20.1.174
```
## Parameter settings
**Performance of PnR varying the size of probe/build table**
```
p=frequency threshold=0.5%
n=number of nodes=3, 6, 12
z=Zipf factor=1.2
|S|=size of build table=10000
|R|=size of probe table=(|R|/|S|)×10000
```
**Performance of PnR varying the degree of skewness**
```
p=frequency threshold=0.5%
n=number of nodes=3, 6, 12
z=Zipf factor (is a variable)
|S|=size of build table=10k (tuples)
|R|=size of probe table=293k (tuples)
```
**Performance of PnR varying the size of the cluster**
```
p=frequency threshold=0.5%
n=number of nodes (is a variable)
z=Zipf factor=1.5
|S|=size of build table=19k (tuples)
|R|=size of probe table=391k (tuples)
```
