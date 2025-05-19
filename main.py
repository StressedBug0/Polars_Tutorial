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
    "J": ["a",'b','c'],
    "D": ['1','2','3']
})

# ========
# Read
# ========

# df = pl.read_csv(path)

# ========
# Look
# ========

print('Head')
print(df.head(10))

print('\n\nTail')
print(df.tail(5))

print('\n\nSample')
print(df.sample(5))

print('\n\nSchema')
print(df.schema)

print('\n\nDescribe')
print(df.describe())

# =================
# Selecting
# =================

print('\n\nSelect Year')
print(df.select("year").head())

print('\n\nYear & Sale')
print(df.select(['year','sales']).head())

print('\n\nYear, Sale, Cost')
print(df.select(['year','sales', 'cost']).describe())

# =================
# Slice & Filter
# =================
print('\n\nSlice')
print(df.slice(2,3))

print('\n\nFilter')
print(df.filter(pl.col("id") == 10))

print('\n\nQuery')
print(
    df.filter(
        (pl.col("sales") > 150) &
        (pl.col("city") == "Boston")
    )
)

# ================
# Add/Modify
# ================
print('\n\nAdd')
df = df.with_columns(
    (pl.col("sales") - pl.col("cost")).alias("profit")
)
print(df.head())

print('\n\nAdd Margin')
df = df.with_columns(
    (pl.col("profit") / pl.col("sales")).alias("margin")
)
print(df.head())

print('\n\nModify Lower')
df = df.with_columns(
    (pl.col("city").str.to_lowercase().alias("city")
))
print(df.head())

print('\n\nRename')
df = df.rename({'city':"City"})
print(df.head())


# =================
# Missing Values
# =================
m = df['margin'].to_list()
for i in range(2,5):
    m[i] = None

df = df.with_columns(pl.Series('margin',m))

print("\n\nNulls:", df['margin'].is_null().sum())

mean_margin = df['margin'].mean()
df = df.with_columns(
    pl.col("margin").fill_null(mean_margin)
)

print("Null:", df['margin'].is_null().sum())
print(df.head(10))

# ====================
# Grouping/Aggregating
# =====================
print("\n\nMean Sales by City")
print(
    df
    .group_by("City")
    .agg(pl.col("sales").mean().alias("mean_sales"))
)

print("\n\nMulti-group")
print(
    df
    .group_by(["City",'year'])
    .agg([
        pl.count("id").alias('orders'),
        pl.sum("sales").alias("revenue"),
        pl.sum("profit").alias('profit')
    ])
    .sort(['City','year'])
)

# ======================
# Sorting & Count
# =======================
print('\n\nTop 10 Profit')
print(
    df
    .sort('profit',descending=True)
    .head(10)
)

print("\n\nCount")
print(df['City'].value_counts())

# ================
# Concat/Merge
# ================
print("\n\nConcat Head and Tail")
part_a = df.head(5)
part_b = df.tail(5)
print('\n', part_a)
print('\n', part_b)
print('\n', pl.concat([part_a,part_b]))

print('\n\nMerge with Lookup')
lookup = pl.DataFrame({
    "City": ['austin','boston','chicago'],
    'population': [100,400,51]
})

print(lookup)

merge_df = df.join(lookup, on='City', how='left')
print(merge_df.select(['City','population','profit']))

# =================
# Save
# ================

# df.write_csv(path)
