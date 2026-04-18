import pandas as pd


def load_bronze_data(path: str) -> pd.DataFrame:
    df=pd.read_csv(path)
    return df



if __name__ == "__main__":
    df=load_bronze_data("data/bronze/ai4i2020.csv")
    print(df.head())