from __future__ import annotations
# External
from enum import Enum
from typing import List, Dict
import polars as pl
import datetime as dt
from pathlib import Path
import pymssql
from functools import reduce

from polars import Expr

# Internal
from src.models.connector import SqlServerConnection, ConnectionType


class Context(Enum):
    PassengerJourney = "PassengerJourney",
    Inventory = "Inventory"


class NewSkiesConnector:
    # Attributes
    _origin: str
    _destination: str
    _start: dt.date
    _end: dt.date
    _connection: pymssql.Connection | None = None
    _cursor: pymssql.Cursor | None = None
    _force_load: bool

    # Core Data
    _raw_data: pl.DataFrame | None  # Raw Customer Journey Data pulled from Database

    def __init__(self, airline_code: str, airline_non_standard_fares: List[str], airline_organization_codes: List[str],
                 origin: str, destination: str,
                 start: dt.date = dt.date.today(),
                 end: dt.date = (dt.datetime.utcnow().today() + dt.timedelta(days=180)), force_load=False):


        # Assign Attributes
        self._airline_code, self._airline_non_standard_fares, self._airline_organization_codes, self._origin, \
            self._destination, self._start, self._end, self._force_load \
            = airline_code, airline_non_standard_fares, airline_organization_codes, \
            origin, destination, start, end, force_load,
        self._initialize_driver()

    def _initialize_driver(self):
        conn_config = SqlServerConnection(ConnectionType.NewSkies)
        self._connection = pymssql.connect(f"{conn_config.uri + ':' + conn_config.port}", conn_config.user,
                                           conn_config.pwd, conn_config.database)
        self._cursor = self._connection.cursor(as_dict=True)

    def _close_driver(self):
        self._connection.close()
        self._connection, self._cursor = None, None

    # Exposed methods ------------------------------------------------------------------------------------------------ #
    def initialize_datasets(self, save_to_local: bool = True):
        print("Getting Data ------------------ #")
        self._pull_raw(save_to_csv=save_to_local)
        self._pull_inventory(save_to_csv=save_to_local)
        print("Done")
        print("Generating journey profile")
        # print("flight_inspect", self._raw_data.filter(
        #     (pl.col("FlightNumber") == " 823")
        #     & (pl.col("STD") == dt.datetime(day=19, month=8, year=2024, hour=17, minute=20))
        # ).sort(["STD", "CreatedUTC"]))
        self._get_group_bookings()  # Generate Group Bookings lookup
        self._get_indirect_journeys()  # Generate Indirect Journeys lookup
        # print("raw_data", self._raw_data)
        self._generate_journey_booking_profile()  # Generate Journey Booking Profile
        print("Datasets initialized")

    def get_naviatire_multi_bookings_view(self) -> pl.DataFrame:
        """
        Get Navitaire multibookings view
        """
        return self._generate_effective_navitaire_multi_bookings_view()

    def get_flight_forecast_view(self):
        """
        Get Flight forecast view
        """
        return self._generate_flight_forecast_view()

    def dataset_print(self):
        with pl.Config(tbl_cols=-1, fmt_str_lengths=100, tbl_width_chars=350):
            print("RawData: ", self._raw_data)
            print("JourneyProfile: ", self._journey_profile)
            print("Indirect: ", self._indirect)
            print("Group:", self._group)

    # Data pull methods ---------------------------------------------------------------------------------------------- #
    # Pull raw data
    @time_it
    def _pull_raw(self, save_to_csv: bool = True) -> pl.DataFrame:
        """
        Bookings + Bookings Passenger + Bookings Baggage + Passenger Journey Segment + Passenger Journey Leg
        + Passenger Journey SSR
        :return: Combination of Bookings + Bookings Passenger + Bookings Baggage
        """
        print("Fetching Passenger Journey Data")
        query_date_format = '%d/%b/%y'
        save_date_format = '%d-%b-%y'
        save_name = f"{self._origin}to{self._destination}.{self._start.strftime(save_date_format)}to" \
                    f"{self._end.strftime(save_date_format)}"

        # Check if model exists, skip if force load is enabled
        if self._file_exists(save_name) and not self._force_load:
            print("Version exists in memory... reading from memory")
            self._raw_data = self._read_csv(save_name, Context.PassengerJourney)
            return self._raw_data

        self._initialize_driver()
        # Fetch data
        self._cursor.execute(self.core_query, (self._origin, self._destination,
                                               self._end.strftime(query_date_format),
                                               self._start.strftime(query_date_format)
                                               ))
        res = self._cursor.fetchall()
        # Convert dictionaries to pandas dataframe
        res_df = pl.DataFrame(res, schema=self._schema)
        self._close_driver()

        # Add standardized keys, these are FlightCodes Journey per passenger per booking Keys and Flight Level Keys
        res_df = res_df.with_columns(pl.concat_str([pl.col("CarrierCode"), pl.col("FlightNumber")]).alias("FlightCode")
                                     .str.strip_chars())
        res_df = res_df.with_columns(pl.concat_str([pl.col("BookingID"), pl.col("PassengerID"), pl.col("SegmentID"),
                                                    pl.col("TripNumber"), pl.col("JourneyNumber"),
                                                    pl.col("SegmentNumber"),
                                                    pl.col("LegNumber")]).str.strip_chars().alias("Key"),
                                     pl.concat_str([pl.col("BookingID"), pl.col("PassengerID"),
                                                    pl.col("TripNumber"), pl.col("JourneyNumber")])
                                     .str.strip_chars().alias("JourneyKey"), pl.concat_str([pl.col("STDUTC"),
                                                                                            pl.col(
                                                                                                "DepartureStationLeg"),
                                                                                            pl.col("ArrivalStationLeg"),
                                                                                            pl.col("FlightCode")],
                                                                                           separator=".")
                                     .str.strip_chars().alias("FlightKey"))

        # Bind Inventory Departure and Passenger Lift Status
        res_df = self.bind_inventory_status(
            self.bind_inventory_departure_status(self.bind_passenger_lift_status(res_df)))

        # Save as class attribute
        self._raw_data = res_df

        # Optionally save as CSV
        if save_to_csv:
            self._write_csv(res_df, save_name)
        return res_df

    # Pull raw data
    @time_it
    def _pull_inventory(self, save_to_csv: bool = True) -> pl.DataFrame:
        """
        Passenger Journey Leg + Inventory + Inventory Nest
        :return: Combination of Bookings + Bookings Passenger + Bookings Baggage
        """
        print("Fetching Inventory Data")
        query_date_format = '%d/%b/%y'
        save_date_format = '%d-%b-%y'
        save_name = f"inventory_{self._origin}to{self._destination}.{self._start.strftime(save_date_format)}to" \
                    f"{self._end.strftime(save_date_format)}"

        # Check if model exists, skip if force load is enabled
        if self._file_exists(save_name) and not self._force_load:
            print("Version exists in memory... reading from memory")
            self._raw_inventory_data = self._read_csv(save_name, Context.Inventory)
            return self._raw_inventory_data

        self._initialize_driver()
        # Fetch data
        self._cursor.execute(self.inventory_query, (self._origin, self._destination,
                                                    self._end.strftime(query_date_format),
                                                    self._start.strftime(query_date_format)
                                                    ))
        res = self._cursor.fetchall()
        # Convert dictionaries to pandas dataframe
        res_df = pl.DataFrame(res, schema=self._inventory_schema)
        self._close_driver()

        # Add standardized keys, these are FlightCodes Journey per passenger per booking Keys and Flight Level Keys
        res_df = res_df.with_columns(pl.concat_str([pl.col("CarrierCode"), pl.col("FlightNumber")]).alias("FlightCode")
                                     .str.strip_chars())
        res_df = res_df.with_columns(pl.concat_str([pl.col("STDUTC"), pl.col("DepartureStation"),
                                                    pl.col("ArrivalStation"),
                                                    pl.col("FlightCode")],
                                                   separator=".")
                                     .str.strip_chars().alias("FlightKey"))

        # Bind Inventory Departure and Passenger Lift Status
        res_df = self.bind_inventory_status(
            self.bind_inventory_departure_status(self.bind_passenger_lift_status(res_df)))

        # Save as class attribute
        self._raw_inventory_data = res_df

        # Optionally save as CSV
        if save_to_csv:
            self._write_csv(res_df, save_name)
        return res_df

    # Data Augmentation Methods -------------------------------------------------------------------------------------- #
    # Generate Journey Booking Profile
    def _generate_journey_booking_profile(self) -> pl.DataFrame:
        # Origin Destination Mapping
        journey_profile = self._raw_data.select(
            ["JourneyKey", "Key", "BookingID", "PassengerID", "SegmentID", "Compartment", "ClassOfService",
             "DepartureStation",
             "ArrivalStation", "TripNumber", "JourneyNumber", "STDUTC", "STAUTC", "PassengerStatus", "FlightStatus",
             "InventoryStatus",
             "BookingStatus", "CarrierCode", "FlightCode", "SourceOrganizationCode"]).unique() \
            .sort(["BookingID", "PassengerID", "TripNumber", "JourneyNumber", "STDUTC"],
                  descending=[False, False, False, False, False]) \
            .group_by(["JourneyKey", "BookingID", "PassengerID", "TripNumber", "JourneyNumber"]) \
            .agg(
            pl.all(),
            (pl.col("DepartureStation").shift(-1) == pl.col("ArrivalStation")).all().alias("ValidJourneyChain"),
            pl.col("DepartureStation").first().alias("JourneyStart"),
            pl.col("STDUTC").first().alias("JourneyStartUTC"),
            pl.col("ArrivalStation").last().alias("JourneyEnd"),
            pl.col("STAUTC").last().alias("JourneyEndUTC"),
            (pl.col("DepartureStation").count() - 1).alias("hops"),
            pl.col("CarrierCode").eq(self._airline_code).any().alias("Serviced"),
            pl.col("CarrierCode").eq(self._airline_code).all().alias("Fulfilled"),
            pl.col("SourceOrganizationCode").is_in(self._airline_organization_codes).all().alias("BookedViaAirline"),
            ~pl.col("ClassOfService").is_in(self._airline_non_standard_fares).all().alias("JourneyStandardAirlineFares")
        )
        # Add Indirect Flag and Group Booking Values
        self._journey_profile = journey_profile.join(self._indirect, on=["PassengerID", "JourneyNumber"], how="left") \
            .with_columns(pl.col("Indirect").fill_null(False)).join(self._group, on=["BookingID"], how="left") \
            .with_columns(pl.col("GroupBooking").fill_null(False)) \
            .sort(["BookingID", "PassengerID", "TripNumber", "JourneyNumber"],
                  descending=[False, False, False, False])

        # with pl.Config(tbl_cols=-1, tbl_rows=50, fmt_str_lengths=100, tbl_width_chars=350):
        #     print("journey_profile", self._journey_profile)

        return self._journey_profile

    # Boost Views ---------------------------------------------------------------------------------------------------- #

    def _generate_wtp_valid_trips(self):
        # Filter by aggregate filters
        target_flights = self._journey_profile.filter(
            self._generate_filters()
        )

        # Filtered Data
        # Filter Trips ## **
        valid_trips = self._raw_data.filter((pl.col("STDUTC") >= self._start)
                                            & (pl.col("STDUTC") <= self._end) &  # Departure Data Range
                                            (pl.col("DepartureStation") == self._origin) &  # Origin Specification
                                            (pl.col("ArrivalStation") == self._destination) &  # Arrival Specification
                                            (~pl.col("Status").is_in([2, 3])) &  # Arrival Specification
                                            (pl.col("FlightNumber").str.strip_chars(" ").str.contains(
                                                r"^\d{1,3}$")))  # Remove Adhoc flights
        if self._filter_config["FlightStandardAirlineFares"]:
            valid_trips.filter(~pl.col("ClassOfService").is_in(self._airline_non_standard_fares))

        target_booking_rows = target_flights.select(pl.col("Key").explode()).get_column("Key").unique()

        # Positive Flights
        filtered_data_airline = valid_trips.filter((pl.col("Key").is_in(
            target_booking_rows.to_list())))  # Filter Relevant Booking Rows

        # BY ANY MEANS DO NOT USE THIS, THIS PART OF THE CODE HAS REGRESSED TO GET STUFF ACROSS,
        # THIS IS A LOW PRIORITY
        # Negative Flights
        filtered_data_others = valid_trips.filter(~pl.col("Key").is_in(filtered_data_airline.get_column("Key"))) \
            .group_by(["FlightKey"]).agg(pl.col("FlightKey").count().alias("LidAdjustment"))

        # with pl.Config(tbl_cols=-1, tbl_rows=50, fmt_str_lengths=100, tbl_width_chars=350):
        #     print("filtered_data", filtered_data_airline)

        self._boost_wtp_valid_store = {"target": filtered_data_airline, "others": []}  # <- I never use this

    # Infare View
    def _generate_effective_navitaire_multi_bookings_view(self) -> pl.DataFrame:
        if self._boost_wtp_valid_store is None:
            self._generate_wtp_valid_trips()

        # Generate Analogue to Navitaire Bookings multi table in JQ legacy ------------------------------------------- #
        # Generate columns not directly present
        bookings_multi = self._boost_wtp_valid_store["target"].with_columns(pl.col("Compartment").alias("compartment"),
                                                                            pl.lit(self._airline_code).alias(
                                                                                "point_of_sale"),
                                                                            pl.lit(self._airline_code).alias(
                                                                                "marketing_airline"),
                                                                            pl.col("FlightNumber").alias(
                                                                                "marketing_flight_number"),
                                                                            pl.lit(True).alias("is_direct"),
                                                                            pl.lit(True).alias("is_confirmed"),
                                                                            pl.col("STD").dt.date().alias(
                                                                                "departure_date"),
                                                                            pl.col("STD").dt.strftime("%H%S").alias(
                                                                                "departure_time"),
                                                                            pl.col("CreatedUTC").dt.date().alias(
                                                                                "creation_date"),
                                                                            pl.lit(0).alias("fare"),
                                                                            pl.lit(dt.date(2088, 8, 8)).alias(
                                                                                "expiry_date"),
                                                                            pl.col("FareBasis").alias(
                                                                                "fare_basis_code"),
                                                                            pl.col("DepartureStation").alias(
                                                                                "trip_origin"),
                                                                            pl.col("ArrivalStation").alias(
                                                                                "trip_destination"),
                                                                            (pl.col("JourneyNumber") - 1).alias(
                                                                                "segment_id"),
                                                                            )

        # Rename columns to be consistent with schema
        bookings_multi = bookings_multi.rename({"RecordLocator": "pnr_record_locator",
                                                "Compartment": "travelled_compartment",
                                                "CarrierCode": "airline",
                                                "FlightNumber": "flight_number",
                                                "DepartureStationLeg": "origin",
                                                "ArrivalStationLeg": "destination",
                                                "ClassOfService": "class",
                                                }).with_row_index("id", offset=0)

        # Generate passenger_id map to be consistent increment of 0,1,2,3....
        # depending of the number of unique passengers per booking
        passenger_id_remap = bookings_multi.select(["BookingID", "PassengerID"]).group_by(["BookingID"]) \
            .agg(pl.col("PassengerID"), pl.col("PassengerID").cum_count().alias("passenger_id")).explode(
            ["PassengerID", "passenger_id"])

        # Bind remapped passenger ids
        bookings_multi = bookings_multi.join(passenger_id_remap, on="PassengerID").drop("id").with_row_index("id",
                                                                                                             offset=0)

        # Select relevant columns
        bookings_multi = bookings_multi.select(
            ["id", "pnr_record_locator", "airline", "flight_number",
             "origin", "destination", "departure_date", "compartment",
             "departure_time", "class", "point_of_sale", "creation_date",
             "expiry_date", "fare", "fare_basis_code",
             "travelled_compartment", "segment_id", "passenger_id",
             "trip_origin", "trip_destination", "marketing_airline",
             "marketing_flight_number", "is_direct", "is_confirmed"
             ]).filter((pl.col("departure_date") >= pl.col("creation_date")) & (
                (pl.col("departure_date") - pl.col("creation_date")).dt.total_days() <= 365) &
                       (pl.col("creation_date") >= (self._start - dt.timedelta(days=365)))
                       ).with_columns(pl.col("flight_number").str.strip_chars(" ").cast(pl.Int32)).with_columns(
            pl.col("marketing_flight_number").str.strip_chars(" ").cast(pl.Int32))

        # with pl.Config(tbl_cols=-1, tbl_rows=50, fmt_str_lengths=100, tbl_width_chars=350):
        #     print("bookings_multi", bookings_multi)

        return bookings_multi

    # Flight forecast view
    def _generate_flight_forecast_view(self):

        # Relevant Inventory to be included via Lid Adjustment based on negative of direct journeys
        target_inventory = self._raw_data.filter(
            (pl.col("STDUTC") >= self._start) &  # Date Range Specification
            (pl.col("STDUTC") <= self._end) &  # Date Range Specification
            (pl.col("CarrierCode") == self._airline_code) &  # Airline Specification
            (pl.col("DepartureStation") == self._origin) &  # Origin Specification
            (pl.col("ArrivalStation") == self._destination) &  # Arrival Specification
            (~pl.col("Status").is_in([2, 3])) &
            (pl.col("FlightNumber").str.strip_chars(" ").str.contains(r"^\d{1,3}$")))

        # if self._filter_config["FlightStandardAirlineFares"]:
        #     self._raw_data.filter(~pl.col("ClassOfService").is_in(self._airline_non_standard_fares))

        with pl.Config(tbl_cols=-1, tbl_rows=1000, fmt_str_lengths=100, tbl_width_chars=350):
            print("flight_forecast", target_inventory.sort(["CarrierCode", "STD", "FlightNumber", "CreatedUTC"]))

        # Join on the number of others passengers was in the booking
        target_inventory = target_inventory.join(self._group, on=["BookingID"], how="left") \
            .with_columns(pl.col("GroupBooking").fill_null(False))

        # Assign a group booking flag if the number of unique passengers in the booking is above a certain threshold
        target_inventory = target_inventory.with_columns(pl.when(pl.col("GroupBooking") == True).then(1)
                                                         .otherwise(0).alias("GroupBooking"),
                                                         pl.lit(1).alias("Booking")) \
            .select(["FlightKey", "STD", "STDUTC", "STA", "Compartment", "GroupBooking", "Booking", "DepartureStation",
                     "ArrivalStation", "CarrierCode", "CreatedUTC", "FlightNumber",
                     "AdjustedCapacity", "Capacity", "Lid"
                     ]).with_columns(pl.col("CreatedUTC").dt.date().alias("CreatedUTC")).sort("CreatedUTC",
                                                                                              descending=False)

        # Group by each day to determine the number of incremental bookings on the day
        flight_forecast = target_inventory.group_by_dynamic("CreatedUTC", every="1d",
                                                            group_by=["FlightKey", "Compartment", "CarrierCode",
                                                                      "FlightNumber", "STD", "STDUTC"]) \
            .agg(pl.col("Booking").sum().alias("bookings"), pl.col("GroupBooking").sum().alias("group_bookings")
                 ).select(
            ["CreatedUTC", "FlightKey", "Compartment", "CarrierCode", "FlightNumber", "STD", "STDUTC", "bookings",
             "group_bookings"])

        # Create a dummy departure day with 0 incremental bookings and group bookings, this is to deal with up-sampling
        flight_forecast_end = flight_forecast.select(
            ["FlightKey", "Compartment", "CarrierCode", "FlightNumber", "STD", "STDUTC"]).unique().with_columns(
            pl.col("STD").dt.date().alias("CreatedUTC"),
            pl.lit(0).alias("bookings"), pl.lit(0).alias("group_bookings")
        ).select(["CreatedUTC", "FlightKey", "Compartment", "CarrierCode", "FlightNumber", "STD", "STDUTC", "bookings",
                  "group_bookings"])

        # Concat and group by so that duplicate capture dates are summed together,
        # this is when there is also an incremental booking on the departure date.
        flight_forecast = pl.concat([flight_forecast, flight_forecast_end]) \
            .sort(["CarrierCode", "Compartment", "FlightNumber", "STD", "CreatedUTC"],
                  descending=[False, False, False, False, False]) \
            .group_by(["CreatedUTC", "FlightKey", "Compartment", "CarrierCode", "FlightNumber", "STD", "STDUTC"]).agg(
            pl.col("bookings").sum(), pl.col("group_bookings").sum()) \
            .sort(["CarrierCode", "FlightNumber", "STD", "CreatedUTC"]) \
            .select(
            ["CreatedUTC", "FlightKey", "Compartment", "CarrierCode", "FlightNumber", "STD", "STDUTC", "bookings",
             "group_bookings"])

        # Generate cumulative bookings
        flight_forecast = flight_forecast.with_columns(pl.col("bookings").cum_sum()
                                                       .over(["CarrierCode", "FlightNumber", "STD"])
                                                       .alias("bookings_cum"),
                                                       pl.col("group_bookings").cum_sum()
                                                       .over(["CarrierCode", "FlightNumber", "STD"])
                                                       .alias("group_bookings_cum")).sort(["CreatedUTC"])

        with pl.Config(tbl_cols=-1, tbl_rows=1000, fmt_str_lengths=100, tbl_width_chars=350):
            print("flight_forecast", flight_forecast.sort(["CarrierCode", "STD", "FlightNumber", "CreatedUTC"]))

        # Upsample the number of flights to fill gaps in capture dates
        flight_forecast = flight_forecast.upsample(time_column="CreatedUTC", every="1d",
                                                   group_by=["FlightNumber", "CarrierCode", "FlightNumber", "STD"],
                                                   maintain_order=True) \
            .select(pl.col("CreatedUTC").forward_fill(), pl.col("FlightKey").forward_fill(),
                    pl.col("Compartment").forward_fill(), pl.col("CarrierCode").forward_fill(),
                    pl.col("FlightNumber").forward_fill(),
                    pl.col("STD").forward_fill(),
                    pl.col("bookings"),
                    pl.col("group_bookings"),
                    pl.col("bookings_cum").forward_fill(), pl.col("group_bookings_cum").forward_fill()) \
            .with_columns(pl.col("bookings").fill_null(0), pl.col("group_bookings").fill_null(0)) \
            .sort(["STD", "CreatedUTC"])

        # Join to retrive compartment capacity, adjusted capacity and lids
        flight_forecast = flight_forecast.join(
            self._raw_inventory_data.select(["FlightKey", "Compartment", "AdjustedCapacity", "STD", "STA",
                                             "Capacity", "Lid", "ClassLid", "ClassAdjustedCapacity"]).unique(),
            how="left", on=["FlightKey", "Compartment"])

        # Form columns of interest to mirror flight forecast
        flight_forecast = flight_forecast.with_columns(pl.lit(self._airline_code).alias("oprg_carr_code"),
                                                       pl.col("STD").dt.date().alias("flt_dep_date_locl"),
                                                       pl.lit(self._origin).alias("leg_orgn_aipc"),
                                                       pl.lit(self._destination).alias("leg_dstn_aipc"),
                                                       pl.col("STD").dt.strftime("%H:%S").alias(
                                                           "leg_dep_time_locl"),
                                                       pl.col("STA").dt.strftime("%H:%S").alias(
                                                           "leg_arvl_time_locl"),
                                                       pl.col("ClassLid").alias("au"),
                                                       pl.lit(0).alias("bid_price"),
                                                       pl.lit(0).alias("dspl_cost"),
                                                       pl.lit(0).alias("achv_dmd"),
                                                       pl.lit(0).alias("seat_index"),
                                                       pl.lit(0).alias("cnst_dmd"),
                                                       pl.lit(dt.date(2088, 8, 8)).alias("export_date"),
                                                       pl.lit(dt.date(2088, 8, 8)).alias("load_date"),
                                                       (pl.col("bookings_cum") - pl.col("group_bookings_cum"))
                                                       .alias("ttl_bkd"),
                                                       (pl.col("STD").dt.date() - pl.col(
                                                           "CreatedUTC")).dt.total_days().alias("days_prior_dep")
                                                       ).sort("CreatedUTC", descending=False)
        flight_forecast = flight_forecast.rename({
            "FlightNumber": "oprg_flt_no",
            "Compartment": "cmpt_code",
            "CreatedUTC": "capture_date",
            "ClassAdjustedCapacity": "adj_cap",
            "ClassLid": "phy_cap",
            "bookings_cum": "ttl_bkd_with_grp"
        })
        # Rearrange and select columns of interest to mirror flight forecast
        flight_forecast = flight_forecast.select(
            ["oprg_carr_code", "oprg_flt_no", "leg_orgn_aipc", "leg_dstn_aipc", "leg_dep_time_locl",
             "leg_arvl_time_locl", "days_prior_dep", "cmpt_code", "flt_dep_date_locl", "capture_date",
             "adj_cap", "phy_cap", "au", "seat_index", "bid_price", "dspl_cost", "ttl_bkd", "ttl_bkd_with_grp",
             "achv_dmd", "cnst_dmd"]
        ).filter((pl.col("days_prior_dep") <= 365) & (
                pl.col("capture_date") >= (self._start - dt.timedelta(days=365)))).with_columns(
            pl.col("oprg_flt_no").str.strip_chars(" "),
            pl.col("adj_cap").cast(pl.Int32),
            pl.col("flt_dep_date_locl").dt.strftime("%Y-%m-%d"))

        # with pl.Config(tbl_cols=-1, tbl_rows=1000, fmt_str_lengths=100, tbl_width_chars=350):
        #     print("flight_forecast", flight_forecast
        #           # .filter((pl.col("flt_dep_date_locl") <= dt.datetime.now())
        #           # & (pl.col("flt_dep_date_locl") >= (dt.datetime.now()
        #           #                      - dt.timedelta(days=2)))
        #           # )
        #           .sort(["flt_dep_date_locl", "capture_date"], descending=[False, False])
        #           # .unique(keep="last", subset=["oprg_carr_code",
        #           #                               "oprg_flt_no",
        #           #                               "flt_dep_date_locl"])##.filter(pl.col("ttl_bkd") <= 80)
        #           .sort(["flt_dep_date_locl"]).filter(pl.col("days_prior_dep") >= 400).unique(keep="first", subset=["oprg_flt_no", "flt_dep_date_locl"])
        #     )
        return flight_forecast

    # Helper Function ------------------------------------------------------------------------------------------------ #
    @staticmethod
    def _write_csv(data: pl.DataFrame, name: str):
        Path("./data/bookings").mkdir(parents=True, exist_ok=True)
        print(f"Saving to {name}.csv")
        data.write_csv(f"./data/bookings/{name}.csv")

    @staticmethod
    def _file_exists(name: str) -> bool:
        if not Path(f"./data/bookings/{name}.csv").is_file():
            return False
        else:
            return True

    def _read_csv(self, name: str, context: Context) -> pl.DataFrame:
        return pl.read_csv(f"./data/bookings/{name}.csv",
                           schema=self._read_schema if context == Context.PassengerJourney else
                           self._read_inventory_schema)

    @staticmethod
    def is_valid_journey_chain(df: pl.DataFrame):
        """
        Checks if a chain of origin and destinations in a journey profile is valid
        """
        shifted_departures = df["DepartureStation"].shift(-1)
        return (shifted_departures[:-1] == df["ArrivalStation"][:-1]).all()

    def bind_passenger_lift_status(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.join(self._lift_status, on="LiftStatus")

    def bind_inventory_departure_status(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.join(self._departure_status, on="DepartureStatus")

    def bind_inventory_status(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.join(self._inventory_status, on="Status")

    # Helper Variables ----------------------------------------------------------------------------------------------- #
    _schema = {
        "FirstName": pl.String, "CreatedUTC": pl.Datetime, "BookingID": pl.String, "RecordLocator": pl.String,
        "PassengerID": pl.String, "SegmentID": pl.String, "Compartment": pl.String, "FareBasis": pl.String,
        "ClassOfService": pl.String,
        "SourceDomainName": pl.String, "SourceDomainCode": pl.String, "SourceOrganizationName": pl.String,
        "SourceOrganizationCode": pl.String, "DepartureStation": pl.String, "ArrivalStation": pl.String,
        "TripNumber": pl.Int32, "JourneyNumber": pl.Int32, "SegmentNumber": pl.Int32, "CreatedLocationCode": pl.String,
        "LegNumber": pl.Int32, "STDUTC": pl.Datetime, "STD": pl.Datetime, "STAUTC": pl.Datetime, "STA": pl.Datetime,
        "CarrierCode": pl.String, "FlightNumber": pl.String, "OperatingFlightNumber": pl.String, "Status": pl.Int32,
        "DepartureStatus": pl.Int32, "DepartureStationLeg": pl.String, "ArrivalStationLeg": pl.String,
        "BookingStatus": pl.String, "LiftStatus": pl.Int32, "Capacity": pl.Int32, "AdjustedCapacity": pl.Int32,
        "Lid": pl.Int32
    }

    _read_schema = {
        "FirstName": pl.String, "CreatedUTC": pl.Datetime, "BookingID": pl.String, "RecordLocator": pl.String,
        "PassengerID": pl.String, "SegmentID": pl.String, "Compartment": pl.String, "FareBasis": pl.String,
        "ClassOfService": pl.String,
        "SourceDomainName": pl.String, "SourceDomainCode": pl.String, "SourceOrganizationName": pl.String,
        "SourceOrganizationCode": pl.String, "DepartureStation": pl.String, "ArrivalStation": pl.String,
        "TripNumber": pl.Int32, "JourneyNumber": pl.Int32, "SegmentNumber": pl.Int32, "CreatedLocationCode": pl.String,
        "LegNumber": pl.Int32, "STDUTC": pl.Datetime, "STD": pl.Datetime, "STAUTC": pl.Datetime, "STA": pl.Datetime,
        "CarrierCode": pl.String, "FlightNumber": pl.String, "OperatingFlightNumber": pl.String, "Status": pl.Int32,
        "DepartureStatus": pl.Int32, "DepartureStationLeg": pl.String, "ArrivalStationLeg": pl.String,
        "BookingStatus": pl.String, "LiftStatus": pl.Int32, "Capacity": pl.Int32, "AdjustedCapacity": pl.Int32,
        "Lid": pl.Int32, "FlightCode": pl.String, "Key": pl.String, "JourneyKey": pl.String, "FlightKey": pl.String,
        "PassengerStatus": pl.String, "FlightStatus": pl.String, "InventoryStatus": pl.String
    }

    _inventory_schema = {
        "PassengerID": pl.String, "SegmentID": pl.String, "Compartment": pl.String,
        "STDUTC": pl.Datetime, "STD": pl.Datetime, "STAUTC": pl.Datetime, "STA": pl.Datetime, "LegNumber": pl.Int32,
        "FlightNumber": pl.String, "OperatingFlightNumber": pl.String, "Status": pl.Int32, "DepartureStatus": pl.Int32,
        "DepartureStation": pl.String, "ArrivalStation": pl.String, "CarrierCode": pl.String,
        "BookingStatus": pl.String, "LiftStatus": pl.Int32, "Capacity": pl.Int32, "AdjustedCapacity": pl.Int32,
        "Lid": pl.Int32, "TravelClassCode": pl.String, "ClassLid": pl.String, "ClassAdjustedCapacity": pl.String
    }

    _read_inventory_schema = {
        "PassengerID": pl.String, "SegmentID": pl.String, "Compartment": pl.String,
        "STDUTC": pl.Datetime, "STD": pl.Datetime, "STAUTC": pl.Datetime, "STA": pl.Datetime, "LegNumber": pl.Int32,
        "FlightNumber": pl.String, "OperatingFlightNumber": pl.String, "Status": pl.Int32, "DepartureStatus": pl.Int32,
        "DepartureStation": pl.String, "ArrivalStation": pl.String, "CarrierCode": pl.String,
        "BookingStatus": pl.String, "LiftStatus": pl.Int32, "Capacity": pl.Int32, "AdjustedCapacity": pl.Int32,
        "Lid": pl.Int32, "TravelClassCode": pl.String, "ClassLid": pl.String, "ClassAdjustedCapacity": pl.String,
        "FlightCode": pl.String, "FlightKey": pl.String, "PassengerStatus": pl.String, "FlightStatus": pl.String,
        "InventoryStatus": pl.String
    }

    def _get_indirect_journeys(self) -> pl.DataFrame:
        """
        Get Indirect Journeys against a passenger in a booking
        """
        self._indirect = self._raw_data.filter((pl.col("SegmentNumber") + pl.col("LegNumber")) > 2).select(
            ["PassengerID", "JourneyNumber"]) \
            .with_columns(pl.lit(True).alias("Indirect")).unique()
        return self._indirect

    def _get_group_bookings(self) -> pl.DataFrame:
        """
        Get No of unique passengers in a single booking
        """
        self._group = self._raw_data.select(["BookingID", "PassengerID"]).unique().group_by(["BookingID"]) \
            .agg(pl.count("PassengerID").alias("GroupBooking")) \
            .filter(pl.col("GroupBooking") > self._filter_config["group_booking_lower_bound"]) \
            .with_columns(pl.lit(True).alias("GroupBooking"))

        # with pl.Config(tbl_cols=-1, tbl_rows=50, fmt_str_lengths=100, tbl_width_chars=350):
        #     print(self._group)

        return self._group

    def _generate_filters(self) -> Expr | bool:
        """
        Generate Filters to get valid bookings to be accounted for
        """
        # REF!
        # airline_fulfilled_point_to_point = self._journey_profile.filter(
        #     (pl.col("ValidJourneyChain") == True) &  # Sensible Journey
        #     (pl.col("StandardAirlineFare") == True) &  # All Standard JQ Fare
        #     (pl.col("Serviced") == True) &  # Serviced via JQ
        #     (pl.col("Fulfilled") == True) &  # Journey Fulfilled via JQ
        #     (pl.col("BookedViaAirline") == True) &  # Booked via JQ
        #     (pl.col("hops") == 0) &  # Indirect Sanity Check
        #     (pl.col("JourneyStart") == self._origin) &  # Origin
        #     (pl.col("JourneyEnd") == self._destination)  # Destination
        # )
        filters = []
        if self._filter_config["WasServiced"]:
            filters.append(
                (pl.col("Serviced") == True)  # Serviced via target airline
            )
        if self._filter_config["FullyServiced"]:
            filters.append(
                (pl.col("Fulfilled") == True)  # Journey fulfilled via target airline
            )
        if self._filter_config["BookedViaAirlineChannels"]:
            filters.append(
                (pl.col("BookedViaAirline") == True)  # Booked via Airline
            )
        if self._filter_config["JourneyStandardAirlineFares"]:
            filters.append(
                (pl.col("JourneyStandardAirlineFares") == True)  # All Standard JQ fares
            )
        if self._filter_config["FilterGroupBookings"]:
            filters.append(
                (pl.col("GroupBooking") == False)  # Filter Group Bookings
            )

        filters.append(
            (pl.col("hops") < self._filter_config["hop_upper_bound"])
        )

        combined_condition = reduce(lambda x, y: x & y, filters)

        return combined_condition

    # Passenger Lift-status
    _lift_status = pl.DataFrame(
        {
            "PassengerStatus": ["Pending", "CheckedIn", "Boarded", "NoShow"],
            "LiftStatus": [0, 1, 2, 3]
        }, schema={"PassengerStatus": pl.String, "LiftStatus": pl.Int32}
    )

    # Inventory Departure-status
    _departure_status = pl.DataFrame(
        {
            "FlightStatus": ["Default", "Cancelled", "Boarding", "SeeAgent", "Delayed", "Departed"],
            "DepartureStatus": [0, 1, 2, 3, 4, 5],
        }, schema={"FlightStatus": pl.String, "DepartureStatus": pl.Int32}
    )

    # Inventory Status
    _inventory_status = pl.DataFrame(
        {
            "InventoryStatus": ["Normal", "Closed", "Canceled", "Suspended", "ClosedPending"],
            "Status": [0, 1, 2, 3, 5],
        }, schema={"InventoryStatus": pl.String, "Status": pl.Int32}
    )

    # SQL Snippets --------------------------------------------------------------------------------------------------- #
    core_query = """
    DECLARE @Origin as VARCHAR(10) = %s
    DECLARE @Destination as VARCHAR(10) = %s
    -- Date Range
    DECLARE @DATEUPPER as DATE = %s
    DECLARE @DATELOWER as DATE = %s

    -- Private
    DECLARE @_DATEUPPER as DATE = DATEADD(DAY, 1, @DATEUPPER)
    DECLARE @_DATELOWER as DATE = DATEADD(DAY, -1, @DATELOWER)
    -- Core filter Definition
    -- Outputs passenger id because passenger id is unique to a booking, avoids an additional join.
    -- Passenger Bookings
    ;WITH BookingFilterLocation AS (
        SELECT DISTINCT pjl.[PassengerID] FROM [REZJQOD01].[Rez].[PassengerJourneyLeg] pjl 
        INNER JOIN  [REZJQOD01].[Rez].[InventoryLeg] il ON pjl.[InventoryLegID] = il.[InventoryLegID] 
                                                            AND il.[DepartureStation] = @Origin AND il.[ArrivalStation] = @Destination 
                                                            AND (il.[STD] >= @_DATELOWER AND il.[STD] <= @_DATEUPPER)
        --INNER JOIN  [REZJQOD01].[Rez].[BookingPassenger] bp ON pjl.[PassengerID] = bp.[PassengerID] --AND (bp.CreatedUTC >  @_DATELOWER AND bp.CreatedUTC < @_DATEUPPER)
        --WHERE (pjl.CreatedUTC > @_DATELOWER AND pjl.CreatedUTC < @_DATEUPPER)
    ),
    -- Sanity Filter
    BookingFilterSanity AS (
        SELECT pjs.[PassengerID] FROM [REZJQOD01].[Rez].[PassengerJourneySegment] pjs 
        INNER JOIN  [REZJQOD01].[Rez].[PassengerJourneyLeg] pjl ON pjl.[SegmentID] = pjs.[SegmentID] --AND (pjl.CreatedUTC > @_DATELOWER AND pjl.CreatedUTC < @_DATEUPPER)
        INNER JOIN  [REZJQOD01].[Rez].[BookingPassenger] pj ON pj.[PassengerID] = pjs.[PassengerID] --AND (pj.CreatedUTC > @_DATELOWER AND pj.CreatedUTC < @_DATEUPPER)
        WHERE (pjs.[SegmentNumber] + pjl.[LegNumber]) > 2
    ),
    -- Bookings
    Bookings AS ( --bpm.[CreatedUTC]
            SELECT  bpm.[BookingID],  bm.[BookingUTC] AS CreatedUTC, bpm.[PassengerID], bpm.[FirstName], bm.[RecordLocator] 
            FROM [REZJQOD01].[Rez].[BookingPassenger] bpm 
            INNER JOIN [REZJQOD01].[Rez].[Booking] bm
            ON bpm.[BookingID] = bm.[BookingID] --AND (bm.CreatedUTC > @_DATELOWER AND bm.CreatedUTC < @_DATEUPPER)
            --WHERE (bpm.CreatedUTC > @_DATELOWER AND bpm.CreatedUTC < @_DATEUPPER)
    ),
    -- // ------------------------------------------------------------------------------------------------------------------------------------------------ // --
    -- Passenger Journey Segment
    PassengerJourneySegment1 AS (
        SELECT pjs.[PassengerID], pjs.[SegmentID], pjs.[CreatedUTC], pjs.[ModifiedUTC], pjs.[DepartureStation], pjs.[ArrivalStation], pjs.[TripType],
                pjs.[TripNumber], pjs.[JourneyNumber], pjs.[SegmentNumber], pjs.[FareComponentNumber], pjs.[BookingStatus], pjs.[FlexibleFare],
                pjs.[FareStatus], pjs.[FareOverrideReasonCode], pjs.[FareBasis], pjs.[ClassOfService], pjs.[CurrencyCode], pjs.[OverbookIndicator], pjs.[ChannelType],
                pjs.[CreatedOrganizationCode], pjs.[CreatedDomainCode], pjs.[CreatedLocationCode], pjs.[SourceOrganizationCode], o.[OrganizationName] AS SourceOrganizationName,
                pjs.[SourceDomainCode], d.[DomainName] as SourceDomainName, pjs.[SourceLocationCode], pjs.[SalesUTC], pjs.[PricingUTC], pjs.[ActivityUTC]
            FROM [REZJQOD01].[Rez].[PassengerJourneySegment] pjs
            LEFT JOIN [REZJQOD01].[Rez].[Organization] o ON o.[OrganizationCode] = pjs.[SourceOrganizationCode]
            LEFT JOIN [REZJQOD01].[Rez].[Domain] d ON d.[DomainCode] = pjs.[SourceDomainCode]
            --WHERE (pjs.CreatedUTC >  @_DATELOWER AND pjs.CreatedUTC < @_DATEUPPER)
    ),
    PassengerJourneySegment AS (
        SELECT pjs1.*
        FROM PassengerJourneySegment1 pjs1
    ),
    PassengerJourneyLeg AS (
        SELECT pjl.[PassengerID], pjl.[SegmentID], iln.[TravelClassCode] AS Compartment, ilc.[ClassOfService], il.[STDUTC], il.[STAUTC], il.[STD], il.[STA], pjl.[LegNumber], ilo.[TailNumber], il.[FlightNumber], 
        il.[OperatingFlightNumber], il.[Status], ilo.[DepartureStatus], il.[DepartureStation], il.[ArrivalStation], il.[CarrierCode], 
        pjl.[CreatedUTC] ,pjl.[ModifiedUTC], pjl.[BookingStatus], pjl.[LiftStatus], pjl.[SeatStatusCode],  il.[Capacity], 
        il.[AdjustedCapacity], il.[Lid]
            FROM [REZJQOD01].[Rez].[PassengerJourneyLeg] pjl 
            LEFT JOIN  [REZJQOD01].[Rez].[InventoryLeg] il ON pjl.[InventoryLegID] = il.[InventoryLegID] 
            --AND (il.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND il.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
            LEFT JOIN  [REZJQOD01].[Rez].[InventoryLegOp] ilo ON pjl.[InventoryLegID] = ilo.[InventoryLegID]
            --AND (ilo.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND ilo.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
            LEFT JOIN
            Rez.InventoryLegClass AS ilc ON ilc.InventoryLegID = il.InventoryLegID
            --AND (ilc.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND ilc.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
            LEFT JOIN
            Rez.InventoryLegNest AS iln ON iln.InventoryLegID = ilc.InventoryLegID AND iln.ClassNest = ilc.ClassNest
        --AND (iln.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND iln.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
            --WHERE (pjl.CreatedUTC > @_DATELOWER AND pjl.CreatedUTC < @_DATEUPPER)
    ),
    -- Passenger Journey Aggregate // Passenger Journey Segment + Passenger Journey Leg
    PassengerJourney AS (
            SELECT pjs.PassengerID, pjs.SegmentID, pjl_c.Compartment, pjs.FareBasis, pjs.ClassOfService, pjs.SourceDomainName, pjs.SourceDomainCode, pjs.SourceOrganizationName, pjs.SourceOrganizationCode,
            pjs.DepartureStation, pjs.ArrivalStation, pjs.TripNumber, pjs.JourneyNumber, pjs.SegmentNumber, pjs.CreatedLocationCode,
            pjl_c.LegNumber, pjl_c.STDUTC, pjl_c.STAUTC, pjl_c.STD, pjl_c.STA, pjl_c.CarrierCode, pjl_c.FlightNumber, pjl_c.OperatingFlightNumber, pjl_c.Status, pjl_c.DepartureStatus,
            pjl_c.DepartureStation AS DepartureStationLeg, pjl_c.ArrivalStation AS ArrivalStationLeg,  pjl_c.BookingStatus, pjl_c.LiftStatus, pjl_c.Capacity, pjl_c.AdjustedCapacity, pjl_c.Lid
            -- Passenger Journey Segment
            -- pjl_c.CreatedUTC,  pjl_c.ModifiedUTC 
            FROM PassengerJourneySegment pjs LEFT JOIN
            -- Passenger Journey Leg
            PassengerJourneyLeg pjl_c ON pjs.SegmentID = pjl_c.SegmentID AND pjs.PassengerID = pjl_c.PassengerID AND pjl_c.ClassOfService = pjs.ClassOfService
    )

    -- Final Query
    SELECT b.FirstName, b.CreatedUTC, b.BookingID, b.RecordLocator, pj.* 
    FROM PassengerJourney pj
    LEFT JOIN Bookings b ON b.PassengerID = pj.PassengerID
    --WHERE pj.PassengerID IN (SELECT BookingFilterLocation.PassengerID FROM BookingFilterLocation)
    INNER JOIN  BookingFilterLocation bfl on bfl.PassengerID = pj.PassengerID
    --ORDER BY b.[BookingID] ASC, pj.[PassengerID] ASC, pj.STDUTC ASC, pj.[JourneyNumber] ASC, pj.[LegNumber] ASC
     """

    inventory_query = """
    -- Origin Destination
    DECLARE @Origin as VARCHAR(10) = %s
    DECLARE @Destination as VARCHAR(10) = %s
    -- Date Range
    DECLARE @DATEUPPER as DATE = %s
    DECLARE @DATELOWER as DATE = %s 

    -- Private
    DECLARE @_DATEUPPER as DATE = DATEADD(MONTH, 14, @DATEUPPER)
    DECLARE @_DATELOWER as DATE = DATEADD(MONTH, -14, @DATELOWER)
    -- Core filter Definition
    -- Outputs passenger id because passenger id is unique to a booking, avoids an additional join.
    -- Passenger Bookings
    ;WITH BookingFilterLocation AS (
        SELECT bp.[PassengerID] FROM [REZJQOD01].[Rez].[PassengerJourneyLeg] pjl 
        INNER JOIN  [REZJQOD01].[Rez].[InventoryLeg] il ON pjl.[InventoryLegID] = il.[InventoryLegID] 
                                                            AND il.[DepartureStation] = @Origin AND il.[ArrivalStation] = @Destination 
                                                            AND (il.[STD] >= @_DATELOWER AND il.[STD] <= @_DATEUPPER)
        INNER JOIN  [REZJQOD01].[Rez].[BookingPassenger] bp ON pjl.[PassengerID] = bp.[PassengerID] -- AND (bp.CreatedUTC >  @_DATELOWER AND bp.CreatedUTC < @_DATEUPPER)
        -- WHERE (pjl.CreatedUTC > @_DATELOWER AND pjl.CreatedUTC < @_DATEUPPER)
    ),
    -- // ------------------------------------------------------------------------------------------------------------------------------------------------ // --
    -- Passenger Journey Leg
    PassengerJourneyLeg AS (
        SELECT pjl.[PassengerID], pjl.[SegmentID], iln.[TravelClassCode] as Compartment, il.[STDUTC], il.[STAUTC], il.[STD], il.[STA], pjl.[LegNumber], ilo.[TailNumber], il.[FlightNumber], 
        il.[OperatingFlightNumber], il.[Status], ilo.[DepartureStatus], il.[DepartureStation], il.[ArrivalStation], il.[CarrierCode], 
        pjl.[CreatedUTC] ,pjl.[ModifiedUTC], pjl.[BookingStatus], pjl.[LiftStatus], pjl.[SeatStatusCode],  il.[Capacity], il.[AdjustedCapacity], il.[Lid],
        iln.[NestType] as TravelClassCode, iln.[Lid] as ClassLid, iln.[AdjustedCapacity] as ClassAdjustedCapacity
          FROM [REZJQOD01].[Rez].[PassengerJourneyLeg] pjl 
          LEFT JOIN  [REZJQOD01].[Rez].[InventoryLeg] il ON pjl.[InventoryLegID] = il.[InventoryLegID] 
          --AND (il.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND il.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
          LEFT JOIN  [REZJQOD01].[Rez].[InventoryLegOp] ilo ON pjl.[InventoryLegID] = ilo.[InventoryLegID]
          --AND (ilo.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND ilo.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
          LEFT JOIN  [REZJQOD01].[Rez].[InventoryLegNest] iln ON pjl.[InventoryLegID] = iln.[InventoryLegID]
          --AND (iln.CreatedUTC > DATEADD(MONTH, -12, @_DATELOWER) AND iln.CreatedUTC < DATEADD(MONTH, 12, @_DATEUPPER))
         -- WHERE (pjl.CreatedUTC > @_DATELOWER AND pjl.CreatedUTC < @_DATEUPPER)
    )

    -- Final Query
    SELECT pjl.*
    FROM PassengerJourneyLeg pjl
    INNER JOIN BookingFilterLocation b ON b.PassengerID = pjl.PassengerID
    """
    # Copy pasta

    # pl.col("item").apply(lambda s: pl.Series(range(len(s)))).alias("item_id")
    # pl.col("PassengerID").unique().cum_count().over("PassengerId").alias("passenger_id")

    # self._boost_wtp_valid_store["target"].select(["FlightKey", "STDUTC", "STD", "STAUTC",
    #                                              "STA", "DepartureStationLeg",
    #                                              "ArrivalStationLeg",
    #                                              "FlightNumber", "Lid"]).unique().join(
    #     self._boost_wtp_valid_store["others"], how="left", on=["FlightKey"]) \
    #     .with_columns((pl.col("Lid") - pl.col("LidAdjustment")).alias("AdjustedLid"))

    # SELECT
    # uwu.oprg_flt_no, uwu.flt_dep_date_locl, uwu.leg_dep_time_locl, uwu.capture_date, uwu.adj_cap, uwu.phy_cap, uwu.ttl_bkd, uwu.ttl_bkd_with_grp
    # FROM(SELECT *, ROW_NUMBER()
    # OVER(PARTITION
    # BY
    # ff.oprg_carr_code,
    # ff.oprg_flt_no,
    # ff.leg_dep_time_locl,
    # ff.flt_dep_date_locl
    # ORDER
    # BY
    # ff.capture_date
    # DESC
    # ) Rank
    # FROM
    # legacy.flt_fcst_recent
    # ff
    # WHERE
    # ff.oprg_carr_code = 'JQ'
    # AND
    # ff.leg_orgn_aipc = 'BNE'
    # AND
    # ff.leg_dstn_aipc = 'SYD'
    # AND
    # ff.flt_dep_date_locl > NOW() - interval
    # '2 days'
    # AND
    # ff.flt_dep_date_locl < NOW()
    # AND
    # ff.capture_date > NOW() - interval
    # '2 days'
    # AND
    # ff.capture_date < NOW()
    # ORDER
    # BY
    # ff.flt_dep_date_locl
    # ASC, ff.capture_date
    # ASC, ff.oprg_flt_no
    # ASC
    # LIMIT
    # 100) uwu
    # WHERE
    # uwu.rank = 1