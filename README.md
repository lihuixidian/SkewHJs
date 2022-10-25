# SKEWHJs
Hello, here is the project code A of the paper "One Size Cannot Fit All: a Self-Adaptive Solution towards Skewed Hash Join in Shared-nothing RDBMSs". We use cockroach(v21.1.13) as the prototype code to implement the entire solution mentioned in the paper. The changed files in the project have been organize to this repository. To run this project, just replace them with the corresponding files in the cockroach.

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
