# assignment-tesla-aug-2022

## Steps to set-up the pipeline:
- $ `conda create --name assignment-tesla-aug-2022 python=3.9`
- $ `conda activate assignment-tesla-aug-2022`
- $ `pip install -r requirements.txt`
- set-up a mysql server , in m case (should be more secure than this):
  - $ `brew install mysql`
  - $ `brew services start mysql`
  - $ `mysql -uroot`

- To run:
  - $ ` python src/job/report_earthquake_probabilities.py`
- To check the notebook used:
  - $ jupyter notebook.
  - Now, visit http://localhost:8888