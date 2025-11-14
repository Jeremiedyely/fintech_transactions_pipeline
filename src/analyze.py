import os
import pandas as pd

# PATH SETUP 
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(THIS_DIR)

CLEAN_PATH = os.path.join(ROOT, "data", "processed", "fraudTrain_cleaned.csv")
OUTPUT_DIR = os.path.join(ROOT, "data", "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def clean_row_values(df: pd.DataFrame, columns) -> pd.DataFrame:
    """
    Clean row values for given columns:
    - replace underscores with spaces
    - convert to Title Case
    Column NAMES are NOT changed, only the cell values.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("_", " ", regex=False)
                .str.title()
            )
    return df


def main():
    print(">>> Running analyze.py <<<")

    # LOAD CLEANED DATA 
    df = pd.read_csv(CLEAN_PATH)
    print(f"Loaded dataset: {len(df)} rows, {len(df.columns)} columns")

    df["transaction_amount"] = pd.to_numeric(df["transaction_amount"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df = df.dropna(subset=["transaction_amount"])

    df["customer_name"] = (
        df["first_name"].fillna("") + " " + df["last_name"].fillna("")
    ).str.strip()

    # TOP CUSTOMERS (SPENDING SUMMARY)
    customer_summary = (
        df.groupby(["credit_card_number", "customer_name"])["transaction_amount"]
        .agg(
            total_spent="sum",
            transaction_count="count",
            average_transaction_amount="mean",
        )
        .reset_index()
        .sort_values("total_spent", ascending=False)
    )

    customer_summary["average_transaction_amount"] = (
        customer_summary["average_transaction_amount"].round(2)
    )

    customer_summary = customer_summary[
        [
            "credit_card_number",
            "customer_name",
            "total_spent",
            "transaction_count",
            "average_transaction_amount",
        ]
    ]

    customer_summary.to_csv(
        os.path.join(OUTPUT_DIR, "customer_transactions.csv"),
        index=False,
    )

    # TOP CATEGORIES (SPENDING SUMMARY)
    category_summary = (
        df.groupby("merchant_category")["transaction_amount"]
        .agg(
            total_spent="sum",
            transaction_count="count",
            average_transaction_amount="mean",
        )
        .reset_index()
        .sort_values("total_spent", ascending=False)
    )

    category_summary = clean_row_values(category_summary, ["merchant_category"])

    category_summary["average_transaction_amount"] = (
        category_summary["average_transaction_amount"].round(2)
    )

    category_summary = category_summary[
        [
            "merchant_category",
            "total_spent",
            "transaction_count",
            "average_transaction_amount",
        ]
    ]

    category_summary.to_csv(
        os.path.join(OUTPUT_DIR, "category_transactions.csv"),
        index=False,
    )

    # WEEKLY / MONTHLY SPENDING PATTERNS
    
    df["week"] = df["transaction_date"].dt.to_period("W").astype(str)
    df["year"] = df["transaction_date"].dt.year
    df["month"] = df["transaction_date"].dt.strftime("%B")

    weekly_patterns = (
        df.groupby(["credit_card_number", "merchant_category", "week"])[
            "transaction_amount"
        ]
        .sum()
        .reset_index()
    )

    weekly_patterns = clean_row_values(weekly_patterns, ["merchant_category"])

    weekly_patterns = weekly_patterns[
        [
            "credit_card_number",
            "merchant_category",
            "week",
            "transaction_amount",
        ]
    ]

    weekly_patterns.to_csv(
        os.path.join(OUTPUT_DIR, "weekly_patterns.csv"),
        index=False,
    )

    # MONTHLY PATTERNS 
    monthly_patterns = (
        df.groupby(["credit_card_number", "merchant_category", "year", "month"])[
            "transaction_amount"
        ]
        .sum()
        .reset_index()
    )

    monthly_patterns = clean_row_values(monthly_patterns, ["merchant_category"])

    monthly_patterns = monthly_patterns[
        [
            "credit_card_number",
            "merchant_category",
            "year",
            "month",
            "transaction_amount",
        ]
    ]

    monthly_patterns.to_csv(
        os.path.join(OUTPUT_DIR, "monthly_patterns.csv"),
        index=False,
    )

    # CATEGORY TIME SERIES (DATE x CATEGORY)
    category_timeseries = (
        df.groupby(["transaction_date", "merchant_category"])["transaction_amount"]
        .sum()
        .reset_index()
        .sort_values("transaction_date")
    )

    # Clean merchant_category row values
    category_timeseries = clean_row_values(category_timeseries, ["merchant_category"])

    category_timeseries = category_timeseries[
        [
            "transaction_date",
            "merchant_category",
            "transaction_amount",
        ]
    ]

    category_timeseries.to_csv(
        os.path.join(OUTPUT_DIR, "category_timeseries.csv"),
        index=False,
    )

    print(">>> Analysis complete. Reports saved to data/outputs/ <<<")


if __name__ == "__main__":
    main()
