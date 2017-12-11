This Scala source code contains the **Chi et al's algorithm** implementation for *MapReduce*. 

It may be run with *cost-sensitive learning* (higher RW for minority class) and rule selection within each Map task. 

The sintax for running the program is the following one:

**spark-submit --class run.fuzzyGenetic --executor-memory 52G --total-executor-cores <#Maps> Chi-Spark-CS-RS-1.0.jar <ParamFile.txt> <dataset_folder> <header_file> <train_file> <test_file> <output_dir>**

The Parameter File includes by default:

inference=winning_rule
num_linguistic_labels=3
cost_sensitive=1
num_individuals=50
num_evaluations=0
alpha=0.7
init_seed=10000
cross_validation=0

**inference** is the aggregation mechanism for determining the output class label, either winning_rule (the one rule with the highest compability degree) of additive_combination (all rules are used).
**num_linguistic_labels** determines the number of partitions for the grid search
**cost_sensitive** refers to whether the RW is updated in accordance with the class distribution (1) or not (0)
**num_individuals**, **num_evaluations**, **alpha**, and **init_seed** are parameters for the CHC procedure in case of Rule Selection (num_evaluations > 0)
**cross_validation** is used to carry out a direct FCV durinng the running of the program (the dataset file is iterated from 1 to the value pointed out here).
