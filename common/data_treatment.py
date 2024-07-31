import pandas as pd
import pycountry

def treat_general_data(df) -> pd.DataFrame:

    df.dropna(axis=1, how='all', inplace=True)
    df.fillna(pd.NA, inplace=True) 
    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x)).replace('', pd.NA)
    df = df.where(pd.notnull(df), None)
    df.drop_duplicates(inplace=True)
    return df

def convert_columns_to_datetime(df, columns) -> pd.DataFrame:
    for column in columns:
        df[column] = pd.to_datetime(df[column], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    return df


def treat_country_code_data(df: pd.DataFrame, country_code_column: str) -> pd.DataFrame:
    def is_valid_country_code(code):
        try:
            code = code.upper()
            return pycountry.countries.get(alpha_2=code) is not None
        except AttributeError:
            return False
        
    df[country_code_column] = df[country_code_column].apply(lambda code: str(code).upper() if is_valid_country_code(code) else "OTHER")
    return df