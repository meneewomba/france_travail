import os
import sys
import mlflow
import pandas as pd
import numpy as np
import joblib
from dotenv import load_dotenv
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.metrics import classification_report, confusion_matrix, mean_absolute_error, r2_score, accuracy_score
from xgboost import XGBClassifier
from Models.mlflow_reports import build_mlflow_report_classifier

from DatabaseCreator.database import get_db_persistent
from utils.preprocessing import  data_loading, get_df, pipelines, tresholds

load_dotenv()







# Connexion à la base déjà faite ici

# Calcul des seuils sur les salaires logarithmiques




    












def train_classifier(X, y, preprocessor):
    model = Pipeline([
        ("preprocess", preprocessor),
        ("classifier", XGBClassifier(
            n_estimators=500,
            learning_rate=0.1,
            max_depth=10,
            n_jobs=-1,
            random_state=42,
            
        ))
    ])
    model.fit(X, y)
    return model

def evaluate_classifier(model, X_test, y_test):
    y_pred = model.predict(X_test)

    print("\n--- Échantillon de prédictions ---")
    sample = pd.DataFrame({
        "Réel": y_test[:10].values,
        "Prévu": y_pred[:10]
    })
    print(sample)

    print("\n--- Matrice de confusion ---")
    print(confusion_matrix(y_test, y_pred))

    print("\n--- Classification report ---")
    print(classification_report(y_test, y_pred))

    print("\n--- Précision ---")
    print(f"Accuracy: {accuracy_score(y_test, y_pred)}")

def train_classifier_models(preprocessor, X_train_min, y_train_min, X_test_min, y_test_min,
                            X_train_max, y_train_max, X_test_max, y_test_max):
    

    models = {
        "model_min.pckl": (X_train_min, y_train_min, X_test_min, y_test_min),
        "model_max.pckl": (X_train_max, y_train_max, X_test_max, y_test_max)
        
    
    }



    for filename, (X_train, y_train, X_test, y_test) in models.items():
        print(f" Entraînement du modèle : {filename}")
        
        model = train_classifier(X_train, y_train, preprocessor)
        
        print(f" Évaluation du modèle : {filename}")
        evaluate_classifier(model, X_test, y_test)  
        
        joblib.dump(model, filename)
        print(f" Modèle sauvegardé : {filename}")

        build_mlflow_report_classifier(
            model_name=filename.replace(".pckl", ""),
            X=X_test, y=y_test,
            model=model
        )

   





def main():
    # Load environment variables
    load_dotenv()

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    # Connect to the database
    cursor, conn = get_db_persistent()

    # Load data
    data = get_df(cursor, conn)
    data, categorical_features, numeric_features = data_loading(data)

    
    

    

    X = data.drop(columns=['job_id', 'min_tier', 'max_tier'])
    y_min = data['min_tier']
    y_max = data['max_tier']

# Split train/test
    X_train_min, X_test_min, y_train_min, y_test_min = train_test_split(X, y_min, test_size=0.2, random_state=42)
    X_train_max, X_test_max, y_train_max, y_test_max = train_test_split(X, y_max, test_size=0.2, random_state=42)

    # Preprocessing
    preprocessor = pipelines(categorical_features, numeric_features)

    pipeline_min = Pipeline([
    ("preprocessor", preprocessor),
    ("classifier", XGBClassifier(random_state=42))
])

    pipeline_max = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", XGBClassifier(random_state=42))
    ])

    # Entraînement
    pipeline_min.fit(X_train_min, y_train_min)
    pipeline_max.fit(X_train_max, y_train_max)

    train_classifier_models(
        preprocessor,
        X_train_min, y_train_min, X_test_min, y_test_min,
        X_train_max, y_train_max, X_test_max, y_test_max
    )


    # Train models
    

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()