# clv-cpa-survival-curves
.

Events considered are "sales" aka "initial billed".

## Usage
```
pipenv install
pipenv shell
python3 app.py train [input file] [model_file]
python3 app.py predict [input file] [output file] [model_file]
```

## Running tests
```
pytest tests/
```

## Assumptions
### input data

* input data passed as a semi-colon separated text file
* required features:
  * ID
  * PERIODICITY: DAY, WEEK or MONTH
  * COUNTRY
  * NETWORK_OPERATOR
  * MAX_BP
  * Billing periods: depending on PERIODICITY and the period number(s) (e.g. WEEK01 to WEEK52)
* for the predictions only the first billing period is required (e.g. WEEK01)

## Logic

### Training
The model estimates survival rate curves based on the training data. This is done by dividing the revenues in any given billing period by the revenues in the first one.

Therefore, the survival rate at a given billing period _t_ for a subscription _i_ is given as

<img src="img/Rate.png" width="200">

This is done on several groupings aka cohort levels:
* COUNTRY
* COUNTRY / NETWORK_OPERATOR (carrier)



Weights are defined to each of those cohort levels / Billing periods. These weights are functions of the number of subscriptions at the billing period and also depend on hierarchical level of the cohort. For example, country/carrier weights are larger than country only weights.

The final model output of the model is a pickled file per cohort level with the mean survival rate curves per category along with their associated weights.

Recency of the subscriptions are considered in the computation of the average curves.


### Prediction

The model predicts LTV curves based on

* initial revenue
* weighted average of

  * the survival rates of all cohort levels (model inputs)
  * regional survival rate curves (keyed by country) akin to Bayesian priors (expert knowledge).



For example, the LTV prediction for a a period t is:


<img src="img/prediction.png" width="6700">

&nbsp;
&nbsp;


The final LTV prediction is the sum of the predictions for all points of the curve (e.g 52 weeks)


## TODO:
* Handle _daily_ and _monthly_ subscription models.
* Improve weights
* Delegate MAX_BP to the input data
