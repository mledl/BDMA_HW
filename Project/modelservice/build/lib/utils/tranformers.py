import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

def drop_columns(df):
    df = df.drop(df.columns, axis=1)
    return df


class CategoricalTransformer( BaseEstimator, TransformerMixin ):

    def __init__( self, categories ):
        self.categories = categories

    def fit( self, X, y = None ):
        return self

    def transform( self, X, y = None ):
        for index, col in enumerate(X.columns):
            X[col] = X[col].apply(lambda x: x if x in self.categories[index] else 'other')
        return X.values

class NumericalTransformer( BaseEstimator, TransformerMixin ):

    def fit( self, X, y = None ):
        return self

    def transform( self, X, y = None ):
        for col in X:
            if X[col].skew() > 1:
                X[col] = np.log10(X[col] + 1 - min(0, X[col].min()))
        return X.values
