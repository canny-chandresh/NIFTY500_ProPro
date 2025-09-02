
import pandera as pa
from pandera import Column, Check
import pandas as pd

DailySchema = pa.DataFrameSchema({
    "Symbol": Column(str, nullable=False),
    "Date": Column(object, nullable=False, coerce=True),
    "Open": Column(float, Check.ge(0), nullable=False),
    "High": Column(float, Check.ge(0), nullable=False),
    "Low":  Column(float, Check.ge(0), nullable=False),
    "Close":Column(float, Check.ge(0), nullable=False),
    "Volume": Column(float, Check.ge(0), nullable=True)
}, coerce=True)

def validate_daily(df: pd.DataFrame)->pd.DataFrame:
    if df is None or df.empty: return df
    return DailySchema.validate(df)
