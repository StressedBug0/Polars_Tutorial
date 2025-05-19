import numpy as np          # Only for the fake data
import polars as pl

# ===================
# Fake Data
# ===================

def make_data(n=50) -> pl.DataFrame:
    rng = np.random.default_rng(seed=0)
    return pl.DataFrame({
        "id": list(range(1, n + 1)),
        "city": rng.choice(["Austin", "Boston", "Chicago"], n).tolist(),
        "year": rng.choice([2023, 2024, 2025], n).tolist(),
        "sales": rng.integers(50, 200, n).tolist(),
        "cost": rng.integers(20, 120, n).tolist(),
    })

df = make_data()

df_t = pl.DataFrame({
    "J": ["Foo", "Fee", "Fa"],
    "A": ["Foo", "Fee", "Fa"],
})

# ===============
# Read
# ===============
# (Polars can also do `pl.read_csv("file.csv")`)

# ====================
# Look
# ====================
print("Head")
print(df.head(10))

print("\n\nTail")
print(df.tail(5))

print("\n\nSample")
print(df.sample(5))

print("\n\nSchema")
print(df.schema)       # columns + dtypes

print("\n\nDescribe")
print(df.describe())   # summary stats

# ============
# Selecting
# ============
print("\n\nyear column head")
print(df.select("year").head(10))

print("\n\nyear & sales")
print(df.select(["year", "sales"]).head(10))

print("\n\nstats on year, sales, cost")
print(df.select(["year", "sales", "cost"]).describe())

# =================
# Row slicing / filtering
# =================
print("\n\nSlice rows 2–4")
print(df.slice(2, 3))  # start, length

print("\n\nFilter id == 10")
print(df.filter(pl.col("id") == 10))

print("\n\nQuery sales>150 & city=='Boston'")
print(
    df.filter(
        (pl.col("sales") > 150) &
        (pl.col("city") == "Boston")
    )
)

# ================
# Add/modify
# ================
print("\n\nAdd profit")
df = df.with_columns(
    (pl.col("sales") - pl.col("cost")).alias("profit")
)
print(df.head())

print("\n\nAdd margin")
df = df.with_columns(
    (pl.col("profit") / pl.col("sales")).alias("margin")
)
print(df.head())

print("\n\nLowercase city")
df = df.with_columns(
    pl.col("city").str.to_lowercase().alias("city")
)
print(df.head())

print("\n\nRename city→City")
df = df.rename({"city": "City"})
print(df.head())

# ==================
# Missing Values
# ==================
print("\n\nIntroduce NAs in margin rows 2–4")
# Polars is immutable, so we build a mask
m = df["margin"].to_numpy()
m[2:5] = np.nan
df = df.with_columns(pl.Series("margin", m))
print(df["margin"].is_null().sum())
print(df.head(6))

print("\n\nFill missing margin with mean")
mean_margin = df["margin"].mean()
df = df.with_columns(
    pl.col("margin").fill_null(mean_margin)
)
print(df.head(6))

# =========================
# Grouping & Aggregating
# =========================
print("\n\nMean sales by City")
print(
    df.groupby("City")
      .agg(pl.col("sales").mean().alias("mean_sales"))
)

print("\n\nMulti-group summary")
summary = (
    df.groupby(["City", "year"])
      .agg([
        pl.count("id").alias("orders"),
        pl.sum("sales").alias("revenue"),
        pl.sum("profit").alias("profit"),
      ])
      .sort(["City","year"])
)
print(summary)

# ==================
# Sorting & Count
# ==================
print("\n\nTop 10 by profit")
print(df.sort("profit", reverse=True).head(10))

print("\n\nCity counts")
print(df["City"].value_counts())

# ===============
# Concat / Merge
# ===============
print("\n\nConcat head & tail")
part_a = df.head(5)
part_b = df.tail(5)
print(pl.concat([part_a, part_b]))

print("\n\nMerge with lookup")
lookup = pl.DataFrame({
    "City": ["austin", "boston", "chicago"],
    "population": [100, 400, 51],
})
merge_df = df.join(lookup, on="City", how="left")
print(merge_df.select(["City", "population", "profit"]))

# ==================
# Save
# ==================
# df.write_csv("output.csv")
