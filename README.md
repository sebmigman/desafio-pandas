# desafio-pandas
Jupyter notebook to answer challenge questions considering a data file with a JSON twitter object separated by CR ("\n")

# Challenge
## install
`pip install -r requirements.txt`
## Set up imports
mocks the argv/argc object coming from 
setups logging module

## Functions
One function per question, some functions have a internal function (hashtags, emojis) to meet the requirement
There's a common function as well to 'reduce' the output of the internal functions (hashtags, emojis) 

## Testing
Dataframe are sorted and returned from the `qX()` function.
In order to answer the question you call the function which returns a dataframe
`df.head(K)` where K is the top

### New
Add data validation task by performing aggregations on smaller datasets
using CLI tools such as `grep`, `jq`, `sort`, `wc`

## Shortcomings
* No Modularity / WIP
* No CLI / WIP
* Localhost processing assumes a relative small file (400MB) /WIP

## Future
* Refactor single notebook into a main with a library/class definition for questions
* Migrate pandas dataframe logic and action to spark to scale the process
* Add CLI options (input file, K values per question, etc.)

# POC
I'm taking the questions in challenge to a direct application in the context of input data being tweets.

Requirements
* Process new tweets as event-driven source
* Update results with new tweets
* Display results for all questions in real time as they evolve

Solution
* [Simulate] input stream by using kafka-console-producer
* Use kafka as communication bus
* Use spark to aggregate the results according to `qX()` functions
* Update top K for criteria X visualization in realtime

## WIP
* Add kafka service container yaml
* Add spark streaming to read using a fixed schema (tweet schema)
* Add script to produce messages on kafka service via bash script
* Add script to consume messages on kafka service via spark
