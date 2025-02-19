# Internal
import time
import traceback

from pymssql import Cursor

from src.models.connector import SqlServerConnection, ConnectionType

import datetime as dt
import pymssql
import polars as pl

# Initialize sql connection
conn_config = SqlServerConnection(ConnectionType.NewSkies)
connection = pymssql.connect(f"{conn_config.uri + ':' + conn_config.port}", conn_config.user,
                                           conn_config.pwd, conn_config.database)

# Core Seed Query
core_query = """
        -- Variable declaration
        DECLARE @Origin VARCHAR(4);
        DECLARE @Destination VARCHAR(4);
        DECLARE @BookingLowerBoundInclusive DATE;
        DECLARE @BookingUpperBoundInclusive DATE;
        DECLARE @TargetTravelClassCode VARCHAR(1);
        DECLARE @ChannelType INT;
        DECLARE @CarrierCode VARCHAR(8);

        -- Define variables
        SET @Origin = %s;
        SET @Destination = %s;
        SET @BookingLowerBoundInclusive = %s;
        SET @BookingUpperBoundInclusive = %s;
        SET @TargetTravelClassCode = %s;
        SET @ChannelType = %s;
        SET @CarrierCode = %s;

        -- Target Inventory Against the Origin, Destination & Route of Interest
        -- This should be inner joined against booking legs as a filter for the target bookings
        WITH InventoryLegFilter AS (
            SELECT 
                il.*
            FROM 
                [REZJQOD01].[Rez].[InventoryLeg] (NOLOCK) il
            WHERE
                il.[CarrierCode] = @CarrierCode AND
                il.[DepartureStation] = @Origin AND
                il.[ArrivalStation] = @Destination AND
                il.Status NOT IN (2,3) AND il.Lid > 50
        ),
        --------------------------------------------------------------------------------------------------------------------------- //
        -- Bookings
        Bookings AS ( --bpm.[CreatedUTC]
                SELECT  bpm.[BookingID],  CONVERT(DATE, bm.[CreatedUTC]) AS CreatedUTC, bpm.[PassengerID], bpm.[FirstName], bm.[RecordLocator], bm.[SourceLocationCode]
                FROM [REZJQOD01].[Rez].[BookingPassenger] (NOLOCK) bpm 
                INNER JOIN [REZJQOD01].[Rez].[Booking] (NOLOCK) bm 
                ON bpm.[BookingID] = bm.[BookingID] --AND (bm.CreatedUTC > @_DATELOWER AND bm.CreatedUTC < @_DATEUPPER)
                WHERE bm.[ChannelType] = @ChannelType AND bm.CreatedUTC >= @BookingLowerBoundInclusive AND bm.CreatedUTC <= @BookingUpperBoundInclusive
        ),
        -- Passenger Journey Segment
        PassengerJourneySegment AS (
            SELECT pjs.[PassengerID], pjs.[SegmentID], pjs.[CreatedUTC], pjs.[ModifiedUTC], pjs.[DepartureStation], pjs.[ArrivalStation], pjs.[TripType],
                    pjs.[TripNumber], pjs.[JourneyNumber], pjs.[SegmentNumber], pjs.[FareComponentNumber], pjs.[BookingStatus], pjs.[FlexibleFare],
                    pjs.[FareStatus], pjs.[FareOverrideReasonCode], pjs.[FareBasis], pjs.[ClassOfService], pjs.[CurrencyCode], pjs.[OverbookIndicator], pjs.[ChannelType],
                    pjs.[CreatedOrganizationCode], pjs.[CreatedDomainCode], pjs.[CreatedLocationCode], pjs.[SourceOrganizationCode],
                    pjs.[SourceDomainCode], pjs.[SourceLocationCode], pjs.[SalesUTC], pjs.[PricingUTC], pjs.[ActivityUTC]
                FROM [REZJQOD01].[Rez].[PassengerJourneySegment] (NOLOCK) pjs
        ),
        PassengerJourneyLeg AS (
            SELECT il.[InventoryLegID], pjl.[PassengerID], pjl.[SegmentID], iln.[TravelClassCode] AS Compartment, ilc.[ClassOfService], il.[STDUTC], il.[STAUTC], il.[STD], il.[STA], pjl.[LegNumber], ilo.[TailNumber], il.[FlightNumber], 
            il.[OperatingFlightNumber], il.[Status], ilo.[DepartureStatus], il.[DepartureStation], il.[ArrivalStation], il.[CarrierCode], 
            pjl.[CreatedUTC] ,pjl.[ModifiedUTC], pjl.[BookingStatus], pjl.[LiftStatus], pjl.[SeatStatusCode],  il.[Capacity], 
            il.[AdjustedCapacity], il.[Lid]
                FROM [REZJQOD01].[Rez].[PassengerJourneyLeg] (NOLOCK) pjl -- Joining on Inventory Leg Filter Here
                INNER JOIN InventoryLegFilter il ON pjl.[InventoryLegID] = il.[InventoryLegID] 
                LEFT JOIN [REZJQOD01].[Rez].[InventoryLegOp](NOLOCK) ilo ON pjl.[InventoryLegID] = ilo.[InventoryLegID]
                LEFT JOIN [REZJQOD01].[Rez].[InventoryLegClass] (NOLOCK) ilc ON ilc.[InventoryLegID] = il.[InventoryLegID]
                LEFT JOIN [REZJQOD01].[Rez].[InventoryLegNest](NOLOCK) iln ON iln.[InventoryLegID] = ilc.[InventoryLegID] AND iln.[ClassNest] = ilc.[ClassNest]
                WHERE iln.[TravelClassCode] = @TargetTravelClassCode
        ),
        -- Passenger Journey Aggregate // Passenger Journey Segment + Passenger Journey Leg
        PassengerJourney AS (
                SELECT pjl_c.InventoryLegID, pjs.PassengerID, pjs.SegmentID, pjl_c.Compartment, pjs.FareBasis, pjs.ClassOfService, pjs.SourceDomainCode, pjs.SourceOrganizationCode,
                pjs.DepartureStation, pjs.ArrivalStation, pjs.TripNumber, pjs.JourneyNumber, pjs.SegmentNumber, pjs.CreatedLocationCode,
                pjl_c.LegNumber, pjl_c.STDUTC, pjl_c.STD, pjl_c.STAUTC, pjl_c.STA, pjl_c.CarrierCode, pjl_c.FlightNumber, pjl_c.OperatingFlightNumber, pjl_c.Status, pjl_c.DepartureStatus,
                pjl_c.DepartureStation AS DepartureStationLeg, pjl_c.ArrivalStation AS ArrivalStationLeg,  pjl_c.BookingStatus, pjl_c.LiftStatus, pjl_c.Capacity, pjl_c.AdjustedCapacity, pjl_c.Lid
                FROM PassengerJourneySegment  pjs INNER JOIN
                PassengerJourneyLeg pjl_c ON pjs.SegmentID = pjl_c.SegmentID AND pjs.PassengerID = pjl_c.PassengerID AND pjl_c.ClassOfService = pjs.ClassOfService
        ),
        MainQuery AS (
            SELECT
                b.FirstName, b.CreatedUTC, b.BookingID, b.RecordLocator, pj.* 
                FROM PassengerJourney pj
                INNER JOIN Bookings b ON b.PassengerID = pj.PassengerID
        ),
        MainQueryLite AS (
            SELECT
                pj.[PassengerID], pj.[SegmentID], pj.[InventoryLegID] 
                FROM PassengerJourney pj
                INNER JOIN Bookings b ON b.PassengerID = pj.PassengerID
        )
"""

# Currency Conversion Query
currency_conversion_query = """
    WITH CurrencyConversion AS (
        SELECT cc.[FromCurrencyCode], cc.[ToCurrencyCode], cc.[ConversionRate] FROM (
            SELECT cc.[FromCurrencyCode], cc.[ToCurrencyCode], cc.[ConversionRate] ,
            RANK() OVER(PARTITION BY cc.[FromCurrencyCode], cc.[ToCurrencyCode] ORDER BY cc.[CreatedDate] DESC) Rank
            FROM [REZJQOD01].[dbo].[CurrencyConversionHistory] (NOLOCK) cc
            WHERE cc.[ToCurrencyCode] = 'AUD'
        ) cc WHERE cc.Rank = 1
    )
SELECT * FROM CurrencyConversion
"""

currency_conversion_schema = {
    "FromCurrencyCode": pl.String,
    "ToCurrencyCode": pl.String,
    "ConversionRate": pl.Float32
}

# Journey Charges
journey_charges_query = """
        SELECT
        pjc.[CreatedUTC],
        Fee.[Description] as ChargeCodeDescription,
        Fee2.[Description] as TicketCodeDescription,
        pjc.[PassengerID], pjc.[SegmentID], mql.[InventoryLegID], pjc.[ChargeNumber], pjc.[ChargeType], pjc.[ChargeCode], pjc.[TicketCode],
        pjc.[ChargeAmount] ChargeAmount,
        pjc.[CurrencyCode]
        FROM [REZJQOD01].[Rez].[PassengerJourneyCharge] (NOLOCK) pjc
        INNER JOIN MainQueryLite mql ON pjc.[PassengerID] = mql.[PassengerID] 
        AND  pjc.[SegmentID] = mql.[SegmentID]
        LEFT JOIN [REZJQOD01].[dbo].[Fee] (NOLOCK) Fee ON pjc.[ChargeCode] = Fee.[FeeCode]
        LEFT JOIN [REZJQOD01].[dbo].[Fee] (NOLOCK) Fee2 ON pjc.[TicketCode] = Fee2.[FeeCode]
"""

journey_charges_query = f"{core_query}\n{journey_charges_query}"

journey_charges_schema = {
    "CreatedUTC": pl.Datetime,
    "ChargeCodeDescription": pl.String,
    "TicketCodeDescription": pl.String,
    "PassengerID": pl.Int32,
    "SegmentID": pl.Int32,
    "InventoryLegID": pl.Int32,
    "ChargeNumber": pl.Int32,
    "ChargeType": pl.Int32,
    "ChargeCode": pl.String,
    "TicketCode": pl.String,
    "ChargeAmount": pl.Float32,
    "CurrencyCode": pl.String
}

# Journey Fees
journey_fees_query = """
        SELECT
        pf.[CreatedUTC],
        pf.[PassengerID], mql.[SegmentID], pf.[FeeNumber], pf.[FeeCode], 
        Fee.[Description] AS FeeCodeDescription,
        pf.[FeeDetail], pf.[FeeType], 
        pf.[FeeOverride], pf.[SSRCode], 
        ssr.[Name] as SSRCodeDescription, 
        pf.[SSRNumber], pf.[InventoryLegID], pf.[ArrivalStation], pf.[DepartureStation],
        pfc.[ChargeNumber], pfc.[ChargeType], 
        pfc.[ChargeDetail],
        pfc.[ChargeAmount] AS ChargeAmount,
        pfc.[CurrencyCode] AS CurrencyCode
        FROM [REZJQOD01].[Rez].[PassengerFee] pf
            INNER JOIN MainQueryLite mql ON mql.[PassengerID] = pf.[PassengerID] AND mql.[InventoryLegID] = pf.[InventoryLegID]
            LEFT JOIN  [REZJQOD01].[dbo].[Fee] Fee (NOLOCK) ON pf.[FeeCode] = Fee.[FeeCode]
            LEFT JOIN [REZJQOD01].[Rez].[PassengerFeeCharge] pfc (NOLOCK) ON pfc.PassengerID=pf.PassengerID AND pfc.FeeNumber=pf.FeeNumber
            LEFT JOIN [REZJQOD01].[dbo].[SSR] (NOLOCK) ssr ON  pf.[SSRCode] = ssr.[SSRCode]
"""

journey_fees_query = f"{core_query}\n{journey_fees_query}"

journey_fees_schema = {
    "CreatedUTC": pl.Datetime,
    "PassengerID": pl.Int32,
    "SegmentID": pl.Int32,
    "FeeNumber": pl.Int32,
    "FeeCode": pl.String,
    "FeeCodeDescription": pl.String,
    "FeeDetail": pl.String,
    "FeeType": pl.Int32,
    "FeeOverride": pl.Int32,
    "SSRCode": pl.String,
    "SSRNumber": pl.Int32,
    "InventoryLegID": pl.Int32,
    "ArrivalStation": pl.String,
    "DepartureStation": pl.String,
    "ChargeNumber": pl.Int32,
    "ChargeType": pl.Int32,
    "ChargeDetail": pl.String,
    "ChargeAmount": pl.Float32,
    "CurrencyCode": pl.String
}

# Bookings
bookings_query = "SELECT * FROM MainQuery"
bookings_query = f"{core_query}\n{bookings_query}"

bookings_schema = {
    "FirstName": pl.String, "CreatedUTC": pl.Date, "BookingID": pl.Int32, "RecordLocator": pl.String,
    "InventoryLegID": pl.Int32, "PassengerID": pl.Int32, "SegmentID": pl.Int32, "Compartment": pl.String,
    "FareBasis": pl.String, "ClassOfService": pl.String, "SourceDomainCode": pl.String, "SourceOrganizationName": pl.String,
    "DepartureStation": pl.String, "ArrivalStation": pl.String, "TripNumber": pl.Int32, "JourneyNumber": pl.Int32,
    "SegmentNumber": pl.Int32, "CreatedLocationCode": pl.String, "LegNumber": pl.Int32,
    "STDUTC": pl.Datetime, "STD": pl.Datetime, "STAUTC": pl.Datetime, "STA": pl.Datetime,
    "CarrierCode": pl.String, "FlightNumber": pl.String, "OperatingFlightNumber": pl.String, "Status": pl.Int32,
    "DepartureStatus": pl.Int32, "DepartureStationLeg": pl.String, "ArrivalStationLeg": pl.String,
    "BookingStatus": pl.String, "LiftStatus": pl.Int32, "Capacity": pl.Int32, "AdjustedCapacity": pl.Int32,
    "Lid": pl.Int32
}

# Temp Table
create_temp_table = """
DROP TABLE IF EXISTS #TempIDTable4dd030da8be24c98;

CREATE TABLE #TempIDTable4dd030da8be24c98
(
    [BookingID] [BIGINT] NOT NULL,
    [SegmentID] [BIGINT] NOT NULL,
    [PassengerID] [BIGINT] NOT NULL,
    [InventoryLegID] [BIGINT] NOT NULL
);
"""

insert_temp_table = """
INSERT INTO #TempIDTable4dd030da8be24c98 (BookingID, SegmentID, PassengerID, InventoryLegID) VALUES (%s, %s, %s, %s)
"""


query_date_format = '%d/%b/%y'

# Parameters
origin = 'OOL'
destination = 'SYD'
BookingLowerBoundInclusive = dt.datetime(year=2023, day=10, month=10).strftime(query_date_format)
BookingUpperBoundInclusive = dt.datetime(year=2024, day=10, month=10).strftime(query_date_format)
TargetTravelClassCode = 'Y'
ChannelType = 2
CarrierCode = 'JQ'

cursor = connection.cursor(as_dict=True)

bookings = pl.read_csv("bookings.csv", schema=bookings_schema)

try:

    query_tuple = (origin, destination, BookingLowerBoundInclusive, BookingUpperBoundInclusive,
                   TargetTravelClassCode, ChannelType, CarrierCode)

    start_time = dt.datetime.now()  # Capture start time
    # Get Bookings
    cursor.execute(bookings_query, query_tuple)

    res = cursor.fetchall()
    bookings = pl.DataFrame(res, schema=bookings_schema)
    bookings.write_csv("bookings.csv")

    duration = (dt.datetime.now() - start_time).total_seconds() / 60
    print("Finish Bookings :", duration)

    # print("Executing")
    # # Insert temp table query
    # write_items = bookings.select(["BookingID", "SegmentID", "PassengerID", "InventoryLegID"])
    # # print(write_items)
    # write_items = [tuple(row) for row in write_items.iter_rows()]
    # cursor.execute(create_temp_table)
    # cursor.executemany(insert_temp_table, write_items)
    # duration = (dt.datetime.now() - start_time).total_seconds() / 60
    # print("Finish Write Temp(minutes) :", duration)

    # Get Passenger Journey Charges
    cursor.execute(journey_charges_query, query_tuple)
    res = cursor.fetchall()
    passenger_journey_charges = pl.DataFrame(res, schema=journey_charges_schema)
    passenger_journey_charges.write_csv("journey_charges.csv")
    duration = (dt.datetime.now() - start_time).total_seconds() / 60
    print("Finish Get Charges(minutes) :", duration)

    # Get Passenger Journey Fees
    cursor.execute(journey_fees_query, query_tuple)
    res = cursor.fetchall()
    passenger_journey_fees = pl.DataFrame(res, schema=journey_fees_schema)
    passenger_journey_fees.write_csv("journey_fees.csv")
    duration = (dt.datetime.now() - start_time).total_seconds() / 60
    print("Finish Get Fees(minutes) :", duration)

    # Currency Conversion
    cursor.execute(currency_conversion_query)
    res = cursor.fetchall()
    currency_conversion = pl.DataFrame(res, schema=currency_conversion_schema)
    currency_conversion.write_csv("currency_conversion.csv")

    cursor.close()
    connection.close()

    duration = (dt.datetime.now() - start_time).total_seconds() / 60

    print("query_duration(minutes) :", duration)
except Exception as err:
    print(traceback.format_exc())
    # Clean up connection to prevent rate limit
    cursor.close()
    connection.close()




cursor.close()
connection.close()
