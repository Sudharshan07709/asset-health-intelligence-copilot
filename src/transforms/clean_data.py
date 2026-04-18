import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    # Rename columns
    df = df.rename(columns={
        "UDI": "asset_id",
        "Product ID": "product_id",
        "Type": "asset_type",
        "Air temperature [K]": "temperature_air",
        "Process temperature [K]": "temperature_process",
        "Rotational speed [rpm]": "speed",
        "Torque [Nm]": "load",
        "Tool wear [min]": "wear",
        "Machine failure": "failure_flag"
    })

    # Create synthetic timestamp
    df["timestamp"] = pd.date_range(
        start="2024-01-01",
        periods=len(df),
        freq="5min"
    )

    # Handle missing values
    df = df.ffill()

    # Remove duplicates
    df = df.drop_duplicates()

    # Validation flag
    df["is_valid"] = df["temperature_air"] > 0

    return df


if __name__ == "__main__":
    from src.ingestion.load_data import load_bronze_data

    df = load_bronze_data("data/bronze/ai4i2020.csv")
    clean_df = clean_data(df)

    # Save Silver data
    clean_df.to_csv("data/silver/clean_ai4i_data.csv", index=False)

    print(clean_df.head())