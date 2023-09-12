# desafio-pandas
Jupyter notebook to answer challenge questions considering a data file with a JSON twitter object separated by CR ("\n")

# Notebook
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

## Shortcomingsz
No odularity 
No CLI
Localhost processing assumes a relative small file (400MB) 

## Future
Refactor single notebook into a main with a library/class definition for questions
Migrate pandas dataframe logic and action to spark to scale the process
Add CLI options (input file, K values per question, etc.)

# WIP
Add kafka service container yaml
Add spark streaming to read using a fixed schema (tweet schema)
Add script to produce messages on kafka service via bash script
Add script to consume messages on kafka service via spark
