import pandas as pd
from unidecode import unidecode

def drop_null(df):
    return df.dropna()

def uppercase(df):
    columns = df.columns
    for column in columns:
        if df[column].dtype == 'object':
            df[column] = df[column].str.upper()
    return df

def remove_accents(df):
    return df.applymap(lambda x: unidecode(str(x)) if isinstance(x, str) else x)

def to_csv(df,sheet):
    df.to_csv(f'{sheet}.csv', index=False)

