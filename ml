Task 1231269: “Submit a compute job to AWS”

Here’s how you should proceed step by step:

🔹 Step 1: Understand the workflows

The task says: “Understand all 3 compute workflows. Execute the manual submission.”
From your earlier screenshots, the three workflows under Data Compute are:

Machine Learning (MLFlow / AWS Batch)

Log Processing (HPC-style / Metrics Pipeline)

CI Workflows (manual submission)

So, you’ll need to show you can run at least one job manually on AWS, and describe the difference between them.

🔹 Step 2: Set up environment

Clone the analytics repo (this is needed for metrics & ML pipelines):

git clone https://github.com/cat-autonomy/plat_aiesdp_analytics.git
cd plat_aiesdp_analytics/training_templates/metrics_pipeline/src


Create a Python venv & install requirements:

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

🔹 Step 3: Run a compute job (example: metrics pipeline)

Edit the experiment name in metrics_pipeline_run.py:

experiment_name = "metricsPipelineDemo_test"


Run the job:

python3 metrics_pipeline_run.py


This will generate metrics and push them to the metrics DB (via API).

You can then go to Superset (https://superset-dev.autonomy.cat.com/) → Dashboard → aiesdp_training to see results.

🔹 Step 4: (Optional) Try MLFlow job

Follow the mlflow_template in the analytics repo.

Run a training job (e.g., linear regression on Iris dataset).

This pushes experiment results into MLFlow tracking.

🔹 Step 5: Document in ADO

In the Discussion/Comments of the ADO task, write something like:

“Cloned analytics repo, set up virtual env, installed dependencies.”

“Submitted metrics pipeline job (metrics_pipeline_run.py) → metrics logged into DB → verified in Superset.”

“Reviewed MLFlow and log-processing workflows; MLFlow uses AWS Batch, metrics pipeline logs metrics via API.”

“Next: can extend to CI workflow submissions.”
