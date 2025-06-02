import sys
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.discriminant_analysis import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from DatabaseCreator.database import get_db_persistent


class RomeCodeGrouper(BaseEstimator, TransformerMixin):
    def __init__(self, top_n=100):
        self.top_n = top_n

    def fit(self, X, y=None):
        rome_counts = pd.Series(X.squeeze()).value_counts()
        self.top_codes_ = set(rome_counts.head(self.top_n).index)
        return self

    def transform(self, X):
        X = pd.Series(X.squeeze())
        return X.apply(lambda x: x if x in self.top_codes_ else 'OTHER').to_frame()

    def get_feature_names_out(self, input_features=None):
        return np.array(["rome_code"])
    
def get_tier(value,  t66):
    if value <= t66:
        return 0
    else:
        return 1

def get_df(cursor,conn):
    try:
        # --- Chargement des données ---
        job_tab = pd.read_sql_query("""
            SELECT a.job_id, a.experience_required, a.experience_length_months, LEFT(a.insee_code,2) as dpt, 
            a.rome_code, a.moving_code, a.candidates_missing,
            b.contract_type_id, b.contract_nature_id, b.partial,
            b.work_duration, b.hours_per_week, c.contract_type, d.moy_min, d.moy_max, d.fallback_level, d.experience_group
        FROM job a 
        JOIN job_contract b ON a.job_id = b.job_id
        JOIN contract_type c on b.contract_type_id = c.contract_type_id
        JOIN avg_salary_rome d on a.job_id = d.job_id
        """, conn)

        salary = pd.read_sql_query("SELECT job_id, min_monthly_salary, max_monthly_salary FROM salary", conn)


    # --- Préparation des données ---
        data = job_tab.merge(salary, on="job_id")

        threshold_min_66= data['min_monthly_salary'].quantile(0.7)
        threshold_max_66= data['max_monthly_salary'].quantile(0.7)

        data["min_tier"] = data["min_monthly_salary"].apply(lambda x: get_tier(x,  threshold_min_66))
        data["max_tier"] = data["max_monthly_salary"].apply(lambda x: get_tier(x, threshold_max_66))
        
        return data
    except Exception as e:
        print(f"Erreur lors du chargement des données : {e}")
        sys.exit(1) 
 
    
def data_loading(data):
    try:
    # --- Chargement des données ---
       
        
        print("Colonnes après merge : ", data.columns.tolist())
        data = data[
            (data["max_monthly_salary"].notna()) &
            (data["min_monthly_salary"].notna()) &
            (data["max_monthly_salary"] > 0) &
            (data["max_monthly_salary"] < 60000) &
            (data["min_monthly_salary"] > 75) &
            (data["min_monthly_salary"] < 60000) &
            (data["experience_length_months"] < 181) &
            (data["hours_per_week"] > 0)
            
        ]
        
        
        categorical_features = [  "rome_code", "dpt", "contract_type_id", "contract_nature_id", "experience_required"]
        numeric_features = ["experience_length_months", "partial", "moving_code", "candidates_missing", "hours_per_week", "work_duration", "moy_min", "moy_max", "fallback_level", "experience_group"]

        return data, categorical_features, numeric_features
    except Exception as e:
        print(f"Erreur lors du chargement des données : {e}")
        sys.exit(1) 






# --- Features ---


   
    

def pipelines(categorical_features, numeric_features):
    # --- Préprocessing ---
    rome_pipeline = Pipeline([
        ("group_top", RomeCodeGrouper(top_n=100)),
        ("onehot", OneHotEncoder(handle_unknown="ignore"))
    ])

    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=True))
    ])

    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    preprocessor = ColumnTransformer([
        ("rome", rome_pipeline, ["rome_code"]),
        ("cat", categorical_transformer, categorical_features),
        ("num", numeric_transformer, numeric_features)
    ])
    return preprocessor



def tresholds(data):
    p50_min = data['min_monthly_salary'].quantile(0.7)
    p50_max = data['max_monthly_salary'].quantile(0.7)
    return p50_min, p50_max