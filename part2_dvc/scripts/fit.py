# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from catboost import CatBoostClassifier
from sklearn.linear_model import LinearRegression
import yaml
import os
import joblib

# обучение модели
def fit_model():
    # Прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # загрузите результат предыдущего шага: initial_data.csv
    data = pd.read_csv('data/initial_data.csv')
    #print(data.columns)
    
    # реализуйте основную логику шага с использованием гиперпараметров
    cat_features = data.select_dtypes(include='object')
    potential_binary_features = cat_features.nunique() == 2

    binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]
    other_cat_features = cat_features[potential_binary_features[~potential_binary_features].index]
    num_features = data.select_dtypes(['float','int'])
    num_features.drop(columns={params['target_col']}, inplace=True)

    # Используем параметры из файла
    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
            ('cat', CatBoostEncoder(return_df=False), other_cat_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = LinearRegression()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    
    # Обучаем на признаках и целевой переменной
    X = data.drop(columns=[params['target_col']])
    y = data[params['target_col']]
    pipeline.fit(X, y)

    # сохраните обученную модель в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)

if __name__ == '__main__':
    fit_model()