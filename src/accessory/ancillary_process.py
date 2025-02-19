from functools import reduce
from typing import Dict, List
import itertools

import polars as pl
from polars import Expr

from src.models.AncillaryParameters import Bundles, Bags, Seats, Meals


# Helper functions
def extract_to_dict(obj) -> Dict[str, List[str]]:
    return {attr: getattr(obj, attr) for attr in dir(obj)
            if not callable(getattr(obj, attr)) and not attr.startswith("__")}


def extract_to_array(obj) -> List[str]:
    return list(itertools.chain.from_iterable(extract_to_dict(obj).values()))

# Load the CSVs
fee_charges = pl.read_csv("journey_fees.csv")
journey_charges = pl.read_csv("journey_charges.csv")
currency_conversion = pl.read_csv("currency_conversion.csv")
bookings = pl.read_csv("bookings.csv").with_columns(pl.col("CreatedUTC").cast(pl.Date))


# Perform Currency Conversions
# Fee Charts
fee_charges = fee_charges.join(currency_conversion.select(["FromCurrencyCode", "ConversionRate"]), left_on='CurrencyCode', right_on='FromCurrencyCode', how="left")
fee_charges = fee_charges.with_columns((pl.col("ChargeAmount")*pl.col("ConversionRate")).alias("ChargeAmount")).drop(["ConversionRate", "CurrencyCode"])

journey_charges = journey_charges.join(currency_conversion.select(["FromCurrencyCode", "ConversionRate"]), left_on='CurrencyCode', right_on='FromCurrencyCode', how="left")
journey_charges = journey_charges.with_columns((pl.col("ChargeAmount")*pl.col("ConversionRate")).alias("ChargeAmount")).drop(["ConversionRate", "CurrencyCode"])


# Bundles Categorizer

def _generate_summary_expression(ancillaryClass) -> Expr:
    """
    Generate Filters to Categorize Bundles
    """
    expression: Expr | None = None

    for ix, (key, value) in enumerate(extract_to_dict(ancillaryClass).items()):
        if ix == 0:
            expression = pl.when(
                pl.col("ContextCode").is_in(value)).then(pl.lit(key))
        else:
            expression = expression.when(pl.col("ContextCode").is_in(value)).then(pl.lit(key))

    expression = expression.otherwise(pl.lit("NotHandled")).alias("ContextDescription")

    return expression

# Bundle Calculations ------------------------------------------------------------------------------------------------ #
# ------------------ NonDynamicBundlesV2 ------------------- #
NonDynamicBundlesV2 = (
    journey_charges
    .filter(
        # Filter down to specific charge codes
        pl.col("ChargeCode").is_in(extract_to_array(Bundles))
    )
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        ##pl.lit(0).cast(pl.Int64).alias("InventoryLegID"),
        pl.col("InventoryLegID"),
        pl.col("ChargeCode").alias("ContextCode"),
        pl.col("ChargeType").alias("ContextType"),
        pl.col("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Charges").alias("SOURCE")
    ])
)
# ------------------ DynamicBundlesV2 ------------------- #
DynamicBundlesV2 = (
    fee_charges
    # Initial Filer this is
    .filter(
        (pl.col("FeeCode").is_in(extract_to_array(Bundles))) &
        (~pl.col("FeeType").is_in([3, 5]))) # Removal of tax charges
    .group_by(["PassengerID", "SegmentID", "InventoryLegID", "FeeCode", "FeeType"])
    .agg([
        pl.sum("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Fees").alias("SOURCE")
    ])
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("FeeCode").alias("ContextCode"),
        pl.col("FeeType").alias("ContextType"),
        pl.col("ChargeAmount"),
        pl.col("SOURCE")
    ])
)
print(NonDynamicBundlesV2)
print(DynamicBundlesV2)
# ------------------ BundlesAggregateV2 ------------------- #
BundlesAggregateV2 = (
    DynamicBundlesV2
    .vstack(NonDynamicBundlesV2)
    .with_columns(
        _generate_summary_expression(Bundles))
    .group_by(["PassengerID", "SegmentID", "InventoryLegID"])
    .agg([
        pl.col("SOURCE"),
        pl.col('ContextCode').alias("ContextCode"),
        pl.col('ContextDescription').alias("ContextDescription"),
        pl.col('ChargeAmount').alias("ChargeAmount"),
        pl.sum("ChargeAmount").alias("TotalCharge"),
    ]).with_columns(pl.lit("Bundles").alias("AncilType"),)
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("ContextCode"),
        pl.col("ContextDescription"),
        pl.col("ChargeAmount"),
        pl.col("TotalCharge"),
        pl.col("SOURCE"),
        pl.col("AncilType")
    ])
)
# Bags --------------------------------------------------------------------------------------------------------------- #
# ------------------ NonDynamicBagsV2 ------------------- #
NonDynamicBagsV2 = (
    journey_charges
    .filter(pl.col("ChargeCode").is_in(extract_to_array(Bags)))  # Replace values
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        #pl.lit(0).cast(pl.Int64).alias("InventoryLegID"),
        pl.col("InventoryLegID"),
        pl.col("ChargeCode").alias("ContextCode"),
        pl.col("ChargeType").alias("ContextType"),
        pl.col("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Charges").alias("SOURCE")
    ])
)

# ------------------ DynamicBagsV2 ------------------- #
DynamicBagsV2 = (
    fee_charges
    .filter(
        (pl.col("FeeCode").is_in(extract_to_array(Bags))) &
        (~pl.col("ChargeType").is_in([3, 5])))
    .group_by(["PassengerID", "SegmentID", "InventoryLegID", "FeeCode", "FeeType"])
    .agg([
        pl.sum("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Fees").alias("SOURCE")
    ])
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("FeeCode").alias("ContextCode"),
        pl.col("FeeType").alias("ContextType"),
        pl.col("ChargeAmount"),
        pl.col("SOURCE")
    ])
)

# ------------------ BagsAggregateV2 ------------------- #
BagsAggregateV2 = (
    DynamicBagsV2
    .vstack(NonDynamicBagsV2)
    .filter(pl.col("ChargeAmount") > 0.50)
    .with_columns(
        _generate_summary_expression(Bags)
    )
    .group_by(["PassengerID", "SegmentID", "InventoryLegID"])
    .agg([
        pl.col("SOURCE"),
        pl.col('ContextCode').alias("ContextCode"),
        pl.col('ContextDescription').alias("ContextDescription"),
        pl.col('ChargeAmount').alias("ChargeAmount"),
        pl.sum("ChargeAmount").alias("TotalCharge"),
    ])
    .with_columns(
        pl.lit("Bags").alias("AncilType"),
    )
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("ContextCode"),
        pl.col("ContextDescription"),
        pl.col("ChargeAmount"),
        pl.col("TotalCharge"),
        pl.col("SOURCE"),
        pl.col("AncilType")
    ])
)
# Seats -------------------------------------------------------------------------------------------------------------- #
# ------------------ DynamicSeatsV2 ------------------- #
DynamicSeatsV2 = (
    fee_charges
    .filter(
        (pl.col("FeeCode").is_in(extract_to_array(Seats)))
        &
        (~pl.col("ChargeType").is_in([3, 5])))
    .group_by(["PassengerID", "SegmentID", "InventoryLegID", "FeeCode", "FeeType"])
    .agg([
        pl.sum("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Fees").alias("SOURCE")
    ])
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("FeeCode").alias("ContextCode"),
        pl.col("ChargeAmount"),
        pl.col("SOURCE")
    ])
    .filter(pl.col("ChargeAmount") > 0.01)
)

SeatsAggregateV2 = (
    DynamicSeatsV2.with_columns(
        _generate_summary_expression(Seats),
    )
    .group_by(["PassengerID", "SegmentID", "InventoryLegID"])
    .agg([
        pl.col("SOURCE"),
        pl.col('ContextCode').alias("ContextCode"),
        pl.col('ContextDescription').alias("ContextDescription"),
        pl.col('ChargeAmount').alias("ChargeAmount"),
        pl.sum("ChargeAmount").alias("TotalCharge"),
    ]).with_columns(
        pl.lit("Seats").alias("AncilType"),
    ).select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("ContextCode"),
        pl.col("ContextDescription"),
        pl.col("ChargeAmount"),
        pl.col("TotalCharge"),
        pl.col("SOURCE"),
        pl.col("AncilType")
    ])
)
# Meals -------------------------------------------------------------------------------------------------------------- #
# ------------------ DynamicMealsV2 ------------------- #
DynamicMealsV2 = (
    fee_charges
    .filter(
        (pl.col("FeeCode").is_in(extract_to_array(Meals)))
        & (~pl.col("ChargeType").is_in([3, 5])))
    .group_by(["PassengerID", "SegmentID", "InventoryLegID", "FeeCode", "FeeType"])
    .agg([
        pl.sum("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Fees").alias("SOURCE")
    ])
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("FeeCode").alias("ContextCode"),
        pl.col("ChargeAmount"),
        pl.col("SOURCE")
    ])
    .filter(pl.col("ChargeAmount") > 0.01)
)

MealsAggregateV2 = (
    DynamicMealsV2.with_columns(
        _generate_summary_expression(Meals),
    ).group_by(["PassengerID", "SegmentID", "InventoryLegID"])
    .agg([
        pl.col("SOURCE"),
        pl.col('ContextCode').alias("ContextCode"),
        pl.col('ContextDescription').alias("ContextDescription"),
        pl.col('ChargeAmount').alias("ChargeAmount"),
        pl.sum("ChargeAmount").alias("TotalCharge"),
    ]).with_columns(
        pl.lit("Meals").alias("AncilType"),
    ).select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("ContextCode"),
        pl.col("ContextDescription"),
        pl.col("ChargeAmount"),
        pl.col("TotalCharge"),
        pl.col("SOURCE"),
        pl.col("AncilType")
    ])
)

# Base Fare ----------------------------------------------------------------------- #
DynamicBaseFare = (
    journey_charges
    .filter(pl.col("ChargeType") == 0)  # Replace values
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        #pl.lit(0).cast(pl.Int64).alias("InventoryLegID"),
        pl.col("InventoryLegID"),
        pl.col("ChargeCode").alias("ContextCode"),
        pl.col("ChargeType").alias("ContextType"),
        pl.col("ChargeAmount").alias("ChargeAmount"),
        pl.lit("Charges").alias("SOURCE")
    ])
)
BaseFareAggregate = (
    DynamicBaseFare
    .with_columns(
        pl.lit("BaseFare").alias("ContextDescription"))
    .group_by(["PassengerID", "SegmentID", "InventoryLegID"])
    .agg([
        pl.col("SOURCE"),
        pl.col('ContextCode').alias("ContextCode"),
        pl.col('ContextDescription').alias("ContextDescription"),
        pl.col('ChargeAmount').alias("ChargeAmount"),
        pl.sum("ChargeAmount").alias("TotalCharge"),
    ]).with_columns(pl.lit("BaseFare").alias("AncilType"),)
    .select([
        pl.col("PassengerID"),
        pl.col("SegmentID"),
        pl.col("InventoryLegID"),
        pl.col("ContextCode"),
        pl.col("ContextDescription"),
        pl.col("ChargeAmount"),
        pl.col("TotalCharge"),
        pl.col("SOURCE"),
        pl.col("AncilType")
    ])
)
# ---------------------------------------------------------------------------------!


with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000, tbl_cols=1000):
    print(BundlesAggregateV2)

with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000, tbl_cols=1000):
    print(SeatsAggregateV2)

with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000, tbl_cols=1000):
    print(BagsAggregateV2)

with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000, tbl_cols=1000):
    print(MealsAggregateV2)

# Combine Bundles, Bags, Seats, and Meals
final_df = BundlesAggregateV2.vstack(BagsAggregateV2).vstack(SeatsAggregateV2).vstack(MealsAggregateV2).vstack(BaseFareAggregate)


final_df = final_df.join(bookings.select(["PassengerID", "CreatedUTC"]), left_on="PassengerID", right_on="PassengerID")

final_df = final_df.explode(["ContextCode", "ContextDescription", "ChargeAmount"])\
    .pivot("ContextDescription", index="CreatedUTC", aggregate_function="sum", values="ChargeAmount")\
    .sort("CreatedUTC")\
    .with_columns(
    (pl.sum_horizontal("STARTER_PLUS", "FLEX", "FLEX_PLUS", "BUSINESS", "BUSINESS_PLUS", "MAX")).round(2).alias("BundleRevenue"),
    (pl.sum_horizontal("PRE_PAID_CHECK_IN", "PRE_PAID_CABIN", "PRE_PAID_OVERSIZED", "POST_PAID_CHECK_IN", "POST_PAID_CABIN", "COUNTER_OVERSIZED", "COUNTER_CABIN", "GATE_CABIN", "EXCESS")).round(2).alias("BaggageRevenue"),
    (pl.col("STF").round(2).alias("SeatRevenue"))
)

# with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000, tbl_cols=1000):
#     print(final_df)

# Final Result
final_df = final_df.select(
    ["CreatedUTC", "BaseFare", "STARTER_PLUS", "FLEX", "FLEX_PLUS", "BUSINESS", "BUSINESS_PLUS", "MAX", "BundleRevenue",
     "PRE_PAID_CHECK_IN", "PRE_PAID_CABIN", "PRE_PAID_OVERSIZED",
     "POST_PAID_CHECK_IN", "POST_PAID_CABIN", "COUNTER_OVERSIZED",
     "COUNTER_CABIN", "GATE_CABIN", "EXCESS", "BaggageRevenue",
     "SeatRevenue"
     ]
).with_columns(
    (pl.sum_horizontal("BundleRevenue", "BaggageRevenue", "SeatRevenue")).round(2).alias("AncilRevenue"),
    (pl.sum_horizontal("BundleRevenue", "BaggageRevenue", "SeatRevenue", "BaseFare")).round(2).alias("TotalRevenue")
)

with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000, tbl_cols=1000):
    print(final_df)