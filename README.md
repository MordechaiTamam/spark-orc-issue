"# spark-orc-issue" 

In order to demonstrate the issue I'v created a test under SparkORCTest.

A short description of the test and the issue:
I'm creating a data set from a list of json files.
The jsons fields are different on each file (one can have fields a and b and another one can have a and c)
When I'm creating a data set from the JSON files , I'm able to perform any operation on that data set (In the test I'm just calling ds.show())

When I'm transforming the JSON files to ORC format and loading them to a Dataset, the ds.show() results with an exception saying that one of the columns is missing
