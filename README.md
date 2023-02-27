# Task
The input is a large file where each line has the next form: "Number. String"  
For example:
```
415. Apple
30432. Something something something
1. Apple
32. Cherry is the best
2. Banana is yellow
```
Both parts can be repeated within the file. You need to get another file as output, where
all rows are sorted.  
Sort Criteria: Compare parts of String first and if match, then Number.

The output of the file in the example above will be the following:
```
1. Apple
415. Apple
2. Banana is yellow
32. Cherry is the best
30432. Something something something
```
You need to write two programs:
1. A utility for creating a test file of a given size.
2. The actual sorter. It must be able to sort files up to 100Gb in size.

# BigSorter

BigSorter.Generator - for generating large files.  
BigSorter.Sorter - for sorting large files.  

Both apps support help command (-h).

## BigSorter.Generator
Pretty straightforward. You can specify file name with -f param and target size in gigabytes with -g param.  
I didn't enforce duplicate strings as they happen naturally.

## BigSorter.Generator
I used external merge sort.  
Check -h command for perfomance fine tuning.  
Default value for max degree of parallelism is Environment.ProcessorCount - 1.  
Default value for max RAM is 5GB. Can be set to 0 to calculate available RAM automatically.  

My PC specs:  
Core(TM) i7-12700H 2.70 GHz  
16,0 GB RAM  
1 TB SSD

My sorting results:  
1GB ~ 28 sec  
10GB ~ 6 min 20 sec  
100GB(~1 billion strings) ~ 1h 15 min  
