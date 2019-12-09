import pandas as pd


def drop_columns(df):
    df = df.drop(df.columns, axis=1)
    return df


def factorize(df):
    for col in df.columns:
        df[col] = pd.factorize(df[col])[0]
    return df
