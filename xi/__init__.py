from collections import *

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from linearmodels.panel import PanelOLS
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from scipy.stats import *


def get_cpi():
    cpi = pd.read_csv("https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCNS")
    cpi.columns = ["year", "cpi"]
    cpi["year"] = cpi["year"].str.split("-").str[0].astype(int)
    cpi = (cpi.groupby(["year"]).median() / 100).reset_index()
    return cpi


def estimate_xi(df, event_var, *, dsf=None):
    spark = SparkSession.builder.getOrCreate()
    df["date"] = pd.to_datetime(df.date)
    df["permno"] = pd.to_numeric(df.permno)
    df = spark.createDataFrame(df)
    df = df.withColumn("date", df["date"].cast(DateType()))
    df = df.cache()

    daily_full = spark.read.parquet(dsf).fillna(0, subset="ret")
    daily_full = daily_full.withColumn("year", F.year(daily_full["date"]))
    daily_full = daily_full.withColumn("weekday", F.dayofweek(daily_full["date"]))
    daily_full = daily_full.withColumn(
        "mktcap", F.abs(daily_full.prc) * daily_full.shrout
    )
    daily_full = daily_full.withColumn("R", daily_full.ret - daily_full.vwretd)
    daily_full = daily_full.withColumn("R2", (daily_full.R) ** 2)
    sig2_ft = daily_full.groupby(["permno", "year"]).agg(F.mean("R2").alias("sig2_ft"))

    daily_full = daily_full.withColumn("date", F.to_date(daily_full.date))

    w = Window().partitionBy(F.col("permno")).orderBy(F.col("date"))

    for d in range(1, 4):
        daily_full = daily_full.withColumn("R+" + str(d), F.lag("R", -d, 0).over(w))

    daily_full = daily_full.withColumn(
        "cumR",
        (1 + daily_full.R) * (1 + daily_full["R+1"]) * (1 + daily_full["R+2"]) - 1,
    )
    daily_full = daily_full.withColumn("log_R2", F.log(daily_full.cumR**2))
    daily_full = daily_full.select(
        ["permno", "date", "cumR", "log_R2", "weekday", "year", "mktcap"]
    )
    daily_full = daily_full.drop_duplicates()

    daily_full = daily_full.cache()

    df = df.groupby(["permno", "date"]).agg(F.countDistinct(event_var).alias("num"))
    # only keep daily returns for stock in df
    daily = daily_full.join(df.select("permno").distinct(), "permno", "inner")
    # merge number of events to daily returns
    daily_df = daily.join(
        df.select("permno", "date", "num"),
        [
            "permno",
            "date",
        ],
        "left",
    )
    #     return daily_df

    daily_df = daily_df.toPandas()
    daily_df["date"] = pd.to_datetime(daily_df.date)
    # create indicator variable for event days
    daily_df["I"] = daily_df["num"].notnull().astype(int)
    # create firm-year dummy
    daily_df["firmyear"] = daily_df.permno * 10000 + daily_df.year
    df = daily_df.set_index(["permno", "date"])
    df.dropna(subset=["log_R2", "I", "firmyear", "weekday"], inplace=True)
    print("start to estimate gamma")
    # res = PanelOLS(df['log_R2'], df['I'], other_effects=df[['firmyear','weekday']]).fit()
    res = PanelOLS(df["log_R2"], df["I"], other_effects=df["firmyear"]).fit()
    gamma = float(res.params)
    print("estimated gamma  " + str(gamma))

    # now, only keep event days
    daily_df = daily_df[lambda df: df["num"].notnull()]
    # merge daily volatility
    daily_df = daily_df.merge(sig2_ft.toPandas(), on=["permno", "year"])
    daily_df = daily_df.merge(
        daily_df.groupby(["permno"])["num"]
        .count()
        .reset_index()
        .rename({"num": "num_event"}, axis=1),
        on=["permno"],
    )

    daily_df = daily_df[daily_df.num_event > 0]
    daily_df = daily_df.merge(
        daily_df.groupby(["permno"])["date"]
        .count()
        .reset_index()
        .rename({"date": "dft"}, axis=1),
        on=["permno"],
    )

    daily_df["dft"] = daily_df.num_event / daily_df.dft
    daily_df.drop("num_event", axis=1, inplace=True)

    delta_ft = 1 - np.exp(-gamma)

    daily_df["epsft2"] = (
        3 * daily_df.sig2_ft / (1 + 3 * daily_df.dft * (np.exp(gamma) - 1))
    )
    daily_df["epsft"] = np.sqrt(daily_df.epsft2)
    daily_df["vj"] = delta_ft * daily_df.cumR + np.sqrt(
        delta_ft
    ) * daily_df.epsft * norm.pdf(
        -np.sqrt(delta_ft) * daily_df.cumR / daily_df.epsft
    ) / (
        1 - norm.cdf(-np.sqrt(delta_ft) * daily_df.cumR / daily_df.epsft)
    )
    daily_df["xi"] = daily_df.vj * daily_df.mktcap

    xi_est = daily_df[daily_df["num"].notnull()]
    xi_est["xi"] = xi_est["xi"] / 1000
    xi_est = xi_est.merge(
        get_cpi(),
        on="year",
        how="left",
    )
    xi_est["xi"] = xi_est.xi / xi_est.cpi
    xi_est["xi_est"] = xi_est.xi / xi_est["num"]

    return xi_est
