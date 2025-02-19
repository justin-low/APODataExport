# Internal
import datetime
from typing import Dict, List

from src.models.connector import SqlServerConnection, ConnectionType
import traceback
import pymssql
import polars as pl
import datetime as dt
import zipfile
import os

# Initialize sql connection
conn_config = SqlServerConnection(ConnectionType.NewSkies)
connection = pymssql.connect(f"{conn_config.uri + ':' + conn_config.port}", conn_config.user,
                                           conn_config.pwd, conn_config.database)


# List of Files to load
treatment_files = [
    "Treatment",
    "TreatmentProductRanked",
    "TreatmentProduct",
    "TreatmentProductInputParameterPivot",
    "TreatmentDistinctRanked"
]
# SQL Source filter relationship
filter_relationship = {
    "Treatment": ["Treatment", "TreatmentProduct", "TreatmentProductInputParameterPivot"],
    "TreatmentDistinctRanked": ["TreatmentDistinctRanked", "TreatmentProductRanked"]
}


def cross_join_inverse(base: Dict[str, List[str]]) -> Dict[str, str]:
    hash_map = {}
    for (k, v) in base.items():
        for x in v:
            hash_map[x] = k

    return hash_map

date_load_schema = {
    "date": pl.Datetime,
    "start": pl.Datetime,
    "end": pl.Datetime,
    "logged_start_id": pl.Int64,
    "logged_start_datetime": pl.Datetime,
    "logged_end_id": pl.Int64,
    "logged_end_datetime": pl.Datetime
}

# Column Relationship
column_relationship = {
    "Treatment": "LoggedUTC",
    "TreatmentDistinctRanked": "MinLoggedUTC"
}

data_types = {
    "Treatment": {
        "TreatmentID": pl.Int64,
        "ClientCode": pl.Categorical, ## Changed from str
        "SamplingRandomNumber": pl.Float64,
        "LoggedUTC": pl.Datetime,
        "SourceCode": pl.Categorical, ## Changed from str
        "TripFirstTravelDate": pl.Datetime,
        "PaxType": pl.Categorical, ## Changed from str
        "ChannelID": pl.Categorical, ## Changed from str
        "RoleName": pl.Categorical, ## Changed from str
        "TripOriginLocationCode": pl.Categorical, ## Changed from str
        "TripDestinationLocationCode": pl.Categorical, ## Changed from str
        "ExternalBookingID": pl.Utf8,
        "NumberOfPassengers": pl.Int32,
        "TotalItineraryPrice": pl.Float64,
        "FareClass": pl.Categorical, ## Changed from str
        "TripType": pl.Int32,
        "Stage": pl.Int32,
        "ValueType": pl.Categorical, ## Changed from str
        "RequestedCurrency": pl.Categorical, ## Changed from str
        "SessionStartDateTime": pl.Datetime
    },
    "TreatmentProductRanked": {
        "TreatmentIDLook": pl.Int64,
        "TreatmentProductSequence": pl.Int32,
        "TreatmentRanked": pl.UInt16, # changed from pl.Int32
        "TreatmentIDBook": pl.Int64,
        "QuantityBooked": pl.Int32, # changed from pl.Int32
        "FinalStage": pl.UInt16, # changed from pl.Int32
        "ProductID": pl.UInt16, # changed from pl.Int32
        "ProductName": pl.Categorical, # changed from pl.Utf8
        "AmountReport": pl.Float64,
        "CurrencyCodeReport": pl.Categorical, # changed from pl.Utf8
        "Amount": pl.Float64,
        "OriginalPrice": pl.Float32, # Changed from pl.Float64
        "CurrencyCode": pl.Categorical, # Changed from pl.Utf8
        "QuantityAvailable": pl.Int32,
        "CalculatedFrom": pl.Int32,
        "GroupCode": pl.Categorical, # Changed from pl.Utf8
        "TreatmentOrdinal": pl.UInt16, # Changed from pl.Int32
        "TreatmentIDLookByProduct": pl.Int64, # Changed from pl.Int64
        "OfferCode": pl.Categorical, # Changed from pl.Utf8
        "OptimizedPoints": pl.Int32,
        "MinLoggedUTC": pl.Datetime,
    },
    "TreatmentProduct": {
        "TreatmentProductSequence": pl.UInt32, # Changed from pl.Int64
        "ProductID": pl.UInt16, # Changed from pl.Int64
        "Amount": pl.Float32, # Changed from pl.Float64
        "OriginalPrice": pl.Float32, # Changed from pl.Float64
        "CurrencyCode": pl.Categorical, # Changed from pl.Utf8
        "QuantityAvailable": pl.Int32,
        "CalculatedFrom": pl.UInt16, # Changed from pl.Int32
        "GroupCode": pl.Categorical, # Changed from pl.Utf8
        "TreatmentOrdinal": pl.UInt16, # Changed from pl.Utf8
        "OfferCode": pl.Categorical, # Changed from pl.Utf8
        "OptimizedPoints": pl.UInt16, # Changed from pl.Utf8
        "TreatmentID": pl.Int64,
        "ClientCode": pl.Categorical, # Changed from pl.Utf8
        "QuantityRequested": pl.UInt32, # Changed from pl.Utf8
        "SegmentProductRatio": pl.Float32, # Changed from pl.Float 64
        "Cost": pl.Float64,
        "LoggedUTC": pl.Datetime,
    },
    "TreatmentProductInputParameterPivot": {
        "TreatmentProductSequence": pl.Int32, # Changed from pl.Int64
        "TreatmentID": pl.Int64,
        "FareClass": pl.Categorical, # changed from pl.Utf8
        "JourneyFareClassOfServiceList": pl.Utf8,  # -- Not Found Dummy Column
        "FareClassID": pl.Int32,  # -- Not Found Dummy Column
        "ProbabilityMatch": pl.Float64,  # -- Not Found Dummy Column
        "BookingWeekday": pl.Categorical,  # -- Not Found Dummy Column # Changed from pl.Utf8
        "output_label1ID": pl.UInt16,  # -- Not Found Dummy Column # Changed from pl.Int32
        "BookingWeekdayID": pl.UInt16,  # -- Not Found Dummy Column # Changed from pl.Int32
        "LabelMatch": pl.Categorical,  # -- Not Found Dummy Column # Changed from pl.Utf8
        "LineOfBusiness": pl.Categorical,  # -- Not Found Dummy Column # Changed from pl.Utf8
        "output_label1": pl.Utf8,  # -- Not Found Dummy Column
        "output_probability1": pl.Float32,  # -- Not Found Dummy Column # Changed from pl.Float64
        "output_probability2": pl.Float32,  # -- Not Found Dummy Column # Changed from pl.Float64
        "LoggedUTC": pl.Datetime
    },
    "TreatmentDistinctRanked": {
        "MinTreatmentID": pl.Int64,
        "SamplingRandomNumber": pl.Float64,
        "MinLoggedUTC": pl.Datetime,
        "MaxLoggedUTC": pl.Datetime,
        "MaxExternalBookingID": pl.Utf8,
        "ExternalBookingID": pl.Utf8,
        "Stage": pl.UInt16, # Changed from pl.Int32
        "TripReturnTravelDate": pl.Datetime,
        "TripFirstTravelDate": pl.Datetime,
        "PaxType": pl.Categorical, # Changed from pl.Categorical
        "ChannelID": pl.Utf8,
        "RoleName": pl.Categorical, # Changed from pl.Utf8
        "TripOriginLocationCode": pl.Categorical, # Changed from pl.Utf8
        "TripDestinationLocationCode": pl.Categorical, # Changed from pl.Utf8
        "NumberOfPassengers": pl.UInt16, # Changed from pl.Int32
        "MinTotalItineraryPrice": pl.Float64,
        "FareClass": pl.Categorical, # Changed from pl.Utf8
        "TripType": pl.UInt16, # Changed from pl.Int32
        "TreatmentRank": pl.UInt16, # Changed from pl.Int32
        "AgentID": pl.Int64, # Changed from pl.Int32
        "CustomerHomeCity": pl.Utf8,
        "MarketingCarrierCode": pl.Categorical,  # Doubled up # Changed from pl.Utf8
        "OperatingCarrierCode": pl.Categorical, # Doubled up # Changed from pl.Utf8
        "ResidentCountryCode": pl.Categorical, # # Changed from pl.Utf8
        "SegmentDestinationCountryCode": pl.Categorical, # Changed from pl.Utf8
        "SegmentOriginLocationCodeTimeZoneOffsetMinutes": pl.Int32,
        "SegmentTravelTime": pl.Datetime,
        "ServiceBundleCode": pl.Categorical, # Changed from pl.Utf8
        "TotalFareWithoutFeeAndTax": pl.Float64,
        "TravelBooked": pl.Boolean,
        "TripDestinationCountryCode": pl.Categorical, # Changed from pl.Utf8
        "TripOriginCountryCode": pl.Categorical, # Changed from pl.Utf8
        # -- "IPAddress": pl.Utf8, -- implicitly commented
        "IsInServiceBundle": pl.Utf8,
        "JourneyTravelTime": pl.Utf8,
        "SegmentDestinationLocationCode": pl.Utf8,
        "SegmentFirstTravelDate": pl.Datetime,
        "SegmentOriginCountryCode": pl.Categorical, # Changed from pl.Utf8
        "SegmentOriginLocationCode": pl.Categorical, # Changed from pl.Utf8
        "TotalFare": pl.Float64,
        "TripOriginLocationCodeTimeZoneOffsetMinutes": pl.Int32,
        "ContryDestinationLocationCode": pl.Categorical,  # -- Not Found Dummy Column and potential typo # Changed from pl.Utf8
        "ContryOriginLocationCode": pl.Categorical,  # -- Not Found Dummy Column and potential typo # Changed from pl.Utf8
        "FirstPassengerHomeCity": pl.Utf8,
        "TestName": pl.Utf8,
        ## "PassengerProgramNumber": pl.Utf8 -- Explicitly commented
        "OverrideSamplingRandomNumber": pl.Utf8,
        ## "PassengerProgramCode": pl.Utf8 -- Explicitly commented
        "PromotionCode": pl.Utf8,
        "CustomDiscountCode": pl.Utf8,  # -- Not Found Dummy Column
        "AssignableSeatsCount": pl.Utf8,  # -- Not Found Dummy Column
        "SegmentEquipmentSalesConfiguration": pl.Utf8,
        "CjDiscountCode": pl.Utf8,  # -- Not Found Dummy Column
        "LoyaltyFilter": pl.Utf8,
        "PricingDate": pl.Utf8,
        "JourneyDestinationLocationCode": pl.Categorical, # Changed from pl.Utf8
        "JourneyDestinationCountryCode": pl.Categorical, # Changed from pl.Utf8
        "JourneyOriginCountryCode": pl.Categorical, # Changed from pl.Utf8
        "JourneyOriginLocationCode": pl.Categorical, # Changed from pl.Utf8
        # -- "PassengerCustomerNumber": pl.Utf8,
        "JourneyMarketingCarrierCodeList": pl.Utf8,
        "JourneyOperatingCarrierCodeList": pl.Utf8,
        "JourneyOperatingFlightNumberList": pl.Utf8,
        "JourneyMarketingFlightNumberList": pl.Utf8,
        # "RecordLocator": pl.Utf8, -- explicitly commented out
        "JourneyFlightType": pl.Categorical, # Changed from pl.Utf8
        "WebSessionID": pl.Utf8,  # -- Not Found Dummy Column
        "JourneySoldLegByTravelClassList": pl.Utf8,  # -- Not Found Dummy Column
        "JourneyConnectingStationsList": pl.Utf8,  # -- Not Found Dummy Column
        "MMB": pl.Utf8,  # -- Not Found Dummy Column
        "SessionStartDateTime": pl.Utf8,
        "Workflow": pl.Categorical, # changed from pl.Utf8
        "ApplyCjDiscount": pl.Utf8,  # -- Not Found Dummy Column
        "ClientName": pl.Utf8,  # -- Not Found Dummy Column
        "SegmentEquipmentType": pl.Categorical, # Changed from pl.Utf8
        "SegmentEquipmentTypeSuffix": pl.Categorical, # Changed from pl.Utf8
        "TotalFareAdjustment": pl.Utf8,
        "TotalFarePoints": pl.Utf8,
        "TotalFarePointsAdjustment": pl.Utf8,
        "TotalOriginalFare": pl.Float64,
        "SourceOrganizationCode": pl.Categorical # -- Not Found Dummy Column # Changed from pl.Utf8
    }
}

# Retreive and Process Data
def retrieve_and_process_data(cursor, sql_query, id_start, id_end, batch_size, data_types, filter_column, date_start,
                              date_end):
    res_df = None
    iter = 0

    # Fetch data in batches and process
    while id_start < id_end:
        max_id = min(id_start + batch_size, id_end)
        start_time = dt.datetime.now()

        # Execute query for current batch
        cursor.execute(sql_query, (id_start, max_id))
        rows = cursor.fetchall()
        print(f'finish iteration {iter} start id: {id_start}; end id: {max_id}; '
              f'duration (minutes): {(dt.datetime.now() - start_time).total_seconds() / 60}')

        id_start = min(max_id + 1, id_end)

        iter += 1

        if not rows:
            continue

        # Convert rows to DataFrame and merge with main DataFrame
        res = pl.DataFrame(rows, schema=data_types)
        if res_df is None:
            res_df = res
        else:
            res_df.extend(res)

    # Handle empty result
    if res_df is None or len(res_df) == 0:
        res_df = pl.DataFrame({i: [] for i in data_types.keys()})

    # Apply column type casting
    res_df = res_df.with_columns([pl.col(col).cast(dtype) for col, dtype in data_types.items()])

    # Apply date filtering
    res_df = res_df.filter((pl.col(filter_column) >= date_start) & (pl.col(filter_column) <= date_end))

    # Remove accessory filter column if it's the last one
    if res_df.columns[-1] == filter_column:
        res_df = res_df.drop([filter_column])

    # Remove tab and newline characters
    table_dtypes = dict(zip(res_df.columns, res_df.dtypes))
    res_df = res_df.with_columns([
        pl.col(col).str.replace_all(r"[\n\t]", " ") if table_dtypes[col] == pl.Utf8 else pl.col(col)
        for col in res_df.columns
    ])

    return res_df

## Save and Zip data
def save_and_zip_data(res_df, i, date_start, date_end, suffix=""):
    # Define file paths
    title_start, title_end = date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")
    csv_path = f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}{suffix}.csv"
    zip_path = f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}{suffix}.zip"

    # Save DataFrame as CSV
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    res_df.write_csv(csv_path, separator=";", quote_style="necessary", include_header=True)

    # Zip the CSV file
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, os.path.basename(csv_path))

    # Remove the original CSV file
    if os.path.exists(csv_path):
        os.remove(csv_path)

# Process and Split Data
def process_data_with_split(cursor, sql_query, id_start, id_end, batch_size, data_types, filter_column, date_start, date_end, i):
    # Calculate the midpoint
    midpoint = (id_start + id_end) // 2

    # Process the first half
    print("Processing first half...")
    res_df_first_half = retrieve_and_process_data(cursor, sql_query, id_start, midpoint, batch_size, data_types, filter_column, date_start, date_end)
    save_and_zip_data(res_df_first_half, i, date_start, date_end, suffix="_part1")

    # Process the second half
    print("Processing second half...")
    res_df_second_half = retrieve_and_process_data(cursor, sql_query, midpoint + 1, id_end, batch_size, data_types, filter_column, date_start, date_end)
    save_and_zip_data(res_df_second_half, i, date_start, date_end, suffix="_part2")

def run():
    # --------------------------------------------------------------------------------- #
    # Inputs
    batch_start = dt.datetime(year=2025, month=1, day=20)
    batch_end = dt.datetime(year=2025, month=1, day=26)

    # ID Look Up Variables
    table_name = "ANAJQDW01.AnalyticsDW.Treatment"
    id_column = "TreatmentID"
    time_column = "LoggedUTC"

    split = False
    cursor = connection.cursor(as_dict=True)

    try:
        # Read Csv
        date_batches = pl.read_csv("target_date_time_ranges_japan.csv", schema=date_load_schema)
        for items in date_batches.filter((pl.col("start") >= batch_start) & (pl.col("start") <= batch_end)).to_dicts():
            # Flatten and inverse filter relationship input
            table_filter_lookup = cross_join_inverse(filter_relationship)
            for i in treatment_files:
                with open(f"./src/RawSQLQueries/{i}.sql") as file:
                    sql_query = file.read()

                # Get Filter key
                filter_key = table_filter_lookup[i]  # id lookup key
                date_start = items["start"]  # Final Date Range Filter
                date_end = items["end"]  # Final Date Range Filter
                id_start = items["logged_start_id"]   # SQL Indexed Filter
                id_end = items["logged_end_id"]  # SQL Indexed Filter
                filter_column = column_relationship[filter_key]  # Final  Date Range Filter Column

                print(f'pulling {i} for {date_start.strftime("%Y-%m-%d")}')

                batch_size = 100000
                if not split:
                    res_df_first_half = retrieve_and_process_data(cursor, sql_query, id_start, id_end, batch_size,
                                                                  data_types[i], filter_column, date_start, date_end)
                    save_and_zip_data(res_df_first_half, i, date_start, date_end, suffix="")
                else:
                    process_data_with_split(cursor, sql_query, id_start,id_end, batch_size,
                                            data_types[i], filter_column, date_start, date_end, i)


    except Exception as err:
        print(traceback.format_exc())
        # Clean up connection to prevent rate limit
        cursor.close()
        connection.close()


    # Close Cursor
    cursor.close()
    # Close Connection
    connection.close()




# # Pull Data from database
#
#             batch_size = 100000
#             iter = 1
#             res_df = None
#             if i != "TreatmentProduct":
#                 while id_start < id_end:
#                     max_id = min(id_start + batch_size, id_end)
#
#                     start_time = dt.datetime.now()  # Capture start time
#                     cursor.execute(sql_query, (id_start, max_id))
#                     rows = cursor.fetchall()
#                     if not rows:
#                         break
#                     # Convert each batch of rows to a list of tuples and extend the main list
#                     res = pl.DataFrame(rows, schema=data_types[i])
#                     if res_df is None:
#                         res_df = res
#                     else:
#                         res_df.extend(res)
#                     print(f'finish iteration {iter} start id: {id_start}; end id: {max_id}; '
#                           f'duration (minutes): {(dt.datetime.now() - start_time).total_seconds() / 60}')
#                     id_start = min(max_id + 1, id_end)
#                     iter += 1
#
#                 # print(i, res_df)
#
#                 # Guard against empty_periods
#                 if res_df is None or len(res_df) == 0:
#                     res_df = pl.DataFrame({i : [] for i in data_types[i].keys()})
#
#                 res_df = res_df.with_columns([pl.col(col).cast(dtype) for col, dtype in data_types[i].items()])
#
#                 # Filter out non_margined DateRanges
#                 res_df = res_df.filter((pl.col(filter_column) >= date_start) & (pl.col(filter_column) <= date_end))
#                 # Drop Column if it's an accessory
#                 if res_df.columns[-1] == filter_column:
#                     res_df.drop(res_df.columns[-1])
#
#                 # tab and newline character filters
#                 table_dtypes = dict(zip(res_df.columns, res_df.dtypes))  # dtype look up
#                 res_df = res_df.with_columns([
#                     pl.col(col).str.replace_all(r"[\n\t]", " ") if table_dtypes[col] == pl.Utf8 else pl.col(col)
#                     for col in res_df.columns
#                 ])
#
#                 # Declare title
#                 title_start, title_end = date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")
#
#                 csv_path = f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}.csv"
#                 zip_path = f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}.zip"
#
#                 # Save and zip files
#                 res_df.write_csv(f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}.csv", separator=";",quote_style="necessary",
#                                  include_header=True)
#                 with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#                     zipf.write(csv_path, os.path.basename(csv_path))
#
#                 # Remove the original CSV file to avoid redundancy
#                 if os.path.exists(csv_path):
#                     os.remove(csv_path)
#             else:
#                 while id_start < id_end:
#                     max_id = min(id_start + batch_size, id_end)
#
#                     start_time = dt.datetime.now()  # Capture start time
#                     cursor.execute(sql_query, (id_start, max_id))
#                     rows = cursor.fetchall()
#                     if not rows:
#                         break
#                     # Convert each batch of rows to a list of tuples and extend the main list
#                     res = pl.DataFrame(rows, schema=data_types[i])
#                     if res_df is None:
#                         res_df = res
#                     else:
#                         res_df.extend(res)
#                     print(f'finish iteration {iter} start id: {id_start}; end id: {max_id}; '
#                           f'duration (minutes): {(dt.datetime.now() - start_time).total_seconds() / 60}')
#                     id_start = min(max_id + 1, id_end)
#                     iter += 1
#
#                     # print(i, res_df)
#
#                     # Guard against empty_periods
#                 if res_df is None or len(res_df) == 0:
#                     res_df = pl.DataFrame({i: [] for i in data_types[i].keys()})
#
#                 res_df = res_df.with_columns([pl.col(col).cast(dtype) for col, dtype in data_types[i].items()])
#
#                 # Filter out non_margined DateRanges
#                 res_df = res_df.filter((pl.col(filter_column) >= date_start) & (pl.col(filter_column) <= date_end))
#                 # Drop Column if it's an accessory
#                 if res_df.columns[-1] == filter_column:
#                     res_df.drop(res_df.columns[-1])
#
#                 # tab and newline character filters
#                 table_dtypes = dict(zip(res_df.columns, res_df.dtypes))  # dtype look up
#                 res_df = res_df.with_columns([
#                     pl.col(col).str.replace_all(r"[\n\t]", " ") if table_dtypes[col] == pl.Utf8 else pl.col(col)
#                     for col in res_df.columns
#                 ])
#
#                 # Declare title
#                 title_start, title_end = date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")
#
#                 csv_path = f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}.csv"
#                 zip_path = f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}.zip"
#
#                 # Save and zip files
#                 res_df.write_csv(f"./TreatmentExport/{i}/{i}_{title_start}_to_{title_end}.csv", separator=";",
#                                  quote_style="necessary",
#                                  include_header=True)
#                 with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#                     zipf.write(csv_path, os.path.basename(csv_path))
#
#                 # Remove the original CSV file to avoid redundancy
#                 if os.path.exists(csv_path):
#                     os.remove(csv_path)