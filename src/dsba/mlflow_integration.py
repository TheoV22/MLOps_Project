import mlflow
import mlflow.xgboost

def start_run(experiment_name="Default_Experiment"):
    """
    Sets up an MLflow experiment and starts a new run, returniing the run object
    """
    mlflow.set_experiment(experiment_name)
    run = mlflow.start_run()
    print(f"MLflow run started under experiment '{experiment_name}'.")
    return run

def log_parameters(params: dict):
    """
    Logs parameters from a dictionary
    """
    for key, value in params.items():
        mlflow.log_param(key, value)
    print("Logged parameters:", params)

def log_metrics(metrics: dict, step=None):
    """
    Logs metrics from a dictionary
    """
    for key, value in metrics.items():
        mlflow.log_metric(key, value, step=step)
    print("Logged metrics:", metrics)

def log_trained_model(model, model_name="model"):
    """
    Logs the trained model
    """
    mlflow.xgboost.log_model(model, model_name)
    print(f"Model logged as '{model_name}'.")
