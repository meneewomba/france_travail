from datetime import datetime
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
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor



from DatabaseCreator.database import get_db_persistent
from utils.preprocessing import RomeCodeGrouper, data_loading, get_df, pipelines, tresholds
from Models.mlflow_reports import build_mlflow_report


# --- Fonction de training ---

def train_model(X, y, preprocessor):
    model = Pipeline([
        ("preprocess", preprocessor),
        ("GB", XGBRegressor(
            n_estimators=1000,   
            learning_rate=0.01,
            max_depth=10,
            random_state=42,
            n_jobs=-1,
            
        ))
    ])


    y_log = np.log(y)

    X_train, X_test, y_train_log, y_test_log = train_test_split(
        X, y_log, test_size=0.2, random_state=42
    )

    
    model.fit(X_train, y_train_log)
    y_pred_log = model.predict(X_test)

    


    y_pred = np.exp(y_pred_log)
    y_test = np.exp(y_test_log)

    

    

    # Évaluation
    
    print("MAE:", round(mean_absolute_error(y_test, y_pred), 2))
    print("R²:", round(r2_score(y_test, y_pred), 2))
    
    
    
    

    # Convertir en DataFrame pour analyse
    df_results = pd.DataFrame({
        "y_true": y_test,
        "y_pred": y_pred,
        "error": abs(y_test - y_pred)
    })

    # Top 10 plus grands écarts
    biggest_errors = df_results.sort_values(by="error", ascending=False).head(10)

    # Un échantillon aléatoire de 10 prédictions
    sample = df_results.sample(10, random_state=42)
    return model, biggest_errors, sample







def train_regressor_models(data, p50_min, p50_max, features, preprocessor):

    # --- Split selon seuil ---



    min_half_low = data[data["min_monthly_salary"] <= p50_min]
    min_half_high = data[data["min_monthly_salary"] > p50_min]

    max_half_low = data[data["max_monthly_salary"] <= p50_max]
    max_half_high = data[data["max_monthly_salary"] > p50_max]


    # --- Entraînement et sauvegarde ---
    models = {
        "model_min_half_low.pckl": (min_half_low[features], min_half_low["min_monthly_salary"]),
        "model_min_half_high.pckl": (min_half_high[features], min_half_high["min_monthly_salary"]),
        "model_max_half_low.pckl": (max_half_low[features], max_half_low["max_monthly_salary"]),
        "model_max_half_high.pckl": (max_half_high[features], max_half_high["max_monthly_salary"]),
        
    
    }

    for filename, (X, y) in models.items():
        print(f"\nTraining {filename}...")
        model, sample, biggest_errors = train_model(X, y, preprocessor)
        joblib.dump(model, filename)
        
        sample_idx = np.random.choice(len(X), size=1000, replace=False)

        build_mlflow_report(
            model_name=filename,
            X=X.iloc[sample_idx],
            y=y.iloc[sample_idx],
            model=model,
            save_dir="reports/regression"
        )

        

        

        print(sample)
        print(biggest_errors)
        print(f"Saved {filename}")
        


def main():
    # --- Environnement et config ---
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)
    
    MLFLOW_TRACKING_URI= os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    print(f"MLFLOW_TRACKING_URI: {MLFLOW_TRACKING_URI}")
    # --- Connexion BDD ---
    cursor, conn = get_db_persistent()
    # --- Chargement des données ---
    data = get_df(cursor, conn)
    data, categorical_features, numeric_features= data_loading(data)

    
   
    
 

    # --- Préprocessing ---
    preprocessor = pipelines(categorical_features, numeric_features)

    p50_min, p50_max = tresholds(data)

  
    # --- Entraînement du modèle final ---
    train_regressor_models(data, p50_min, p50_max, categorical_features + numeric_features, preprocessor)

    # --- Cleanup ---
    cursor.close()
    conn.close()



if __name__ == "__main__":
    main()