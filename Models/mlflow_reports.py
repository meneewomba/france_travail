




from datetime import datetime, timezone
import os

import mlflow
from mlflow.models.signature import infer_signature
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, mean_squared_error, r2_score



def build_mlflow_report(model_name: str,
                        X: pd.DataFrame, y: pd.Series,
                        model,
                        save_dir: str = "reports/regression"):
    """
    Génère un rapport pour un modèle de régression en utilisant MLflow.
    Les résultats du modèle (comme les erreurs et le R2) sont suivis dans MLflow,
    et un rapport détaillé est sauvegardé dans un fichier texte.
    
    Args:
        model_name: nom du modèle (utilisé pour le nom du fichier)
        X_ref: features jeu de référence
        y_ref: vraies valeurs du jeu de référence
        X_cur: features jeu actuel (évaluation)
        y_cur: vraies valeurs du jeu actuel
        model: pipeline entraîné (incluant preprocessing + prédicteur)
        save_dir: dossier de sortie des rapports
    """
    
    # Prédictions 
    y_pred = model.predict(X)
    
    # Calcul des métriques
    mae = mean_absolute_error(y, y_pred)
    mse = mean_squared_error(y, y_pred)
    r2 = r2_score(y, y_pred)

    

    # Création du dossier pour sauvegarder le rapport
    os.makedirs(save_dir, exist_ok=True)

    # Nommage du fichier pour le rapport
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    text_path = os.path.join(save_dir, f"{model_name}_{timestamp}_report.txt")


    mlflow.set_experiment("regression_experiment")
    # Démarre une exécution MLflow pour suivre les métriques
    with mlflow.start_run():
        # Sauvegarde du modèle dans MLflow
        
        signature = infer_signature(X, model.predict(X))

        mlflow.set_tag("run_type", "regression")
        mlflow.log_param("model_name", model_name)

        mlflow.xgboost.log_model(
        xgb_model=model.named_steps["GB"],  
        artifact_path="model",
        
        signature=signature
)


        
        
        

        # Journalisation des métriques dans MLflow
       
        # Log des métriques
        mlflow.log_param("model_name", model_name)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)

        # Sauvegarde des résultats dans un fichier texte
        with open(text_path, "w") as f:
            f.write("Rapport de régression\n")
            f.write(f"MAE: {mae:.4f}\n")
            f.write(f"MSE: {mse:.4f}\n")
            f.write(f"R2: {r2:.4f}\n")


        # Journalisation du fichier de rapport dans les artefacts MLflow
        mlflow.log_artifact(text_path)

        print(f"Rapport MLflow généré pour {model_name} : {text_path}")


def build_mlflow_report_classifier(model_name: str,
                                    X: pd.DataFrame, y: pd.Series,
                                   model,
                                   save_dir: str = "reports/classification"):
    """
    Génère un rapport pour un classifieur en utilisant MLflow.
    Les résultats du modèle (comme l'accuracy et les métriques) sont suivis dans MLflow,
    et un rapport détaillé au format texte est sauvegardé dans le répertoire spécifié.
    """
    # Prédictions
    y_pred = model.predict(X)

    # Calcul des métriques de classification
    accuracy = accuracy_score(y, y_pred)
   

    # Générer un rapport détaillé des performances (classification report)
    report = classification_report(y, y_pred, output_dict=True)
    
    # Création du dossier pour sauvegarder le rapport
    os.makedirs(save_dir, exist_ok=True)

    # Nommage des fichiers pour le rapport
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    text_path = os.path.join(save_dir, f"{model_name}_{timestamp}_report.txt")

    mlflow.set_experiment("classification_experiment")
        
    # Démarre une exécution MLflow pour suivre les métriques
    with mlflow.start_run():

        signature = infer_signature(X, model.predict(X))

        mlflow.set_tag("run_type", "classification")
        mlflow.log_param("model_name", model_name)
        
        input_example = X.iloc[0:1]
        # Sauvegarde du modèle dans MLflow
        mlflow.xgboost.log_model(
        xgb_model=model.named_steps["classifier"],  # ou ["classifier"] selon ton pipeline
        artifact_path="model",
        signature=signature
)

        

        

        # Journalisation des métriques dans MLflow
        mlflow.log_metric("accuracy", accuracy)
        

        # Journalisation des métriques détaillées dans MLflow
        for metric, value in report.items():
            if isinstance(value, dict):  # ignore les informations de support et autres
                for sub_metric, sub_value in value.items():
                    mlflow.log_metric(f"ref_{metric}_{sub_metric}", sub_value)

        

        # Sauvegarde du rapport dans un fichier texte
        with open(text_path, 'w') as f:
            f.write("Rapport de classification - Données de référence\n")
            f.write(classification_report(y, y_pred))
            

        # Journalisation du fichier de rapport dans les artefacts MLflow
        mlflow.log_artifact(text_path)

        print(f" Rapport MLflow généré pour {model_name} : {text_path}")