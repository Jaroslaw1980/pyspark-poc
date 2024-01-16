# Programming Exercise using PySpark

## Background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate
datasets dealing with clients that they want to collate to starting interfacing more with
their clients. One dataset contains information about the clients and the other one contains
information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom
and the Netherlands and some of their financial details to starting reaching out to them
for a new marketing push.

Since all the data in the datasets is fake and this is just an exercise,
one can forego the issue of having the data stored along with the code in a code repository.

### Examples
```
python main.py --file_one raw_data/dataset_one.csv --file_two raw_data/dataset_two.csv --countries "France" "Netherlands"

python main.py --file_one raw_data/dataset_one.csv -file_two raw_data/dataset_two.csv --countries "United Kingdom"
```

### Input data schemas
- data schema for dataset_one.csv:

        |id|first_name|last_name|email|country|
        |--|----------|---------|-----|-------|

- data schema for dataset_two.csv:

        |id|btc_a|cc_t|cc_n|
        |--|-----|----|----|

### Output data schema

        |client_identifier|email|country|bitcoin_address|credit_card_type|
        |-----------------|-----|-------|---------------|----------------|