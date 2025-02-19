# Internal
import datetime
import time
from typing import Dict, List

from src.models.connector import SqlServerConnection, ConnectionType
import traceback
import pymssql
import polars as pl
import datetime as dt
import zipfile
import os
from collections import defaultdict

from pathlib import Path

treatment_files = [
    #"Treatment", #-- one with issues
    #"TreatmentProductRanked",
    "TreatmentProduct", #--- the one with issues
    #"TreatmentProductInputParameterPivot",
    #"TreatmentDistinctRanked"
]

data_types = {
    "Treatment": {
        "TreatmentID": pl.Int64,
        "ClientCode": pl.Utf8,
        "SamplingRandomNumber": pl.Float64,
        "LoggedUTC": pl.Datetime,
        "SourceCode": pl.Utf8,
        "TripFirstTravelDate": pl.Datetime,
        "PaxType": pl.Utf8,
        "ChannelID": pl.Utf8,
        "RoleName": pl.Utf8,
        "TripOriginLocationCode": pl.Utf8,
        "TripDestinationLocationCode": pl.Utf8,
        "ExternalBookingID": pl.Utf8,
        "NumberOfPassengers": pl.Int32,
        "TotalItineraryPrice": pl.Float64,
        "FareClass": pl.Utf8,
        "TripType": pl.Int32,
        "Stage": pl.Int32,
        "ValueType": pl.Utf8,
        "RequestedCurrency": pl.Utf8,
        "SessionStartDateTime": pl.Datetime
    },
    "TreatmentProductRanked": {
        "TreatmentIDLook": pl.Int64,
        "TreatmentProductSequence": pl.Int32,
        "TreatmentRanked": pl.Int32,
        "TreatmentIDBook": pl.Int64,
        "QuantityBooked": pl.Float64,
        "FinalStage": pl.Int32,
        "ProductID": pl.Int32,
        "ProductName": pl.Utf8,
        "AmountReport": pl.Float64,
        "CurrencyCodeReport": pl.Utf8,
        "Amount": pl.Float64,
        "OriginalPrice": pl.Float64,
        "CurrencyCode": pl.Utf8,
        "QuantityAvailable": pl.Int32,
        "CalculatedFrom": pl.Int32,
        "GroupCode": pl.Utf8,
        "TreatmentOrdinal": pl.Int32,
        "TreatmentIDLookByProduct": pl.Int64,
        "OfferCode": pl.Utf8,
        "OptimizedPoints": pl.Int32,
        "MinLoggedUTC": pl.Datetime,
    },
    "TreatmentProduct": {
        "TreatmentProductSequence": pl.Int64,
        "ProductID": pl.Int64,
        "Amount": pl.Float64,
        "OriginalPrice": pl.Float64,
        "CurrencyCode": pl.Utf8,
        "QuantityAvailable": pl.Int32,
        "CalculatedFrom": pl.Int32,
        "GroupCode": pl.Utf8,
        "TreatmentOrdinal": pl.Utf8,
        "OfferCode": pl.Utf8,
        "OptimizedPoints": pl.Utf8,
        "TreatmentID": pl.Int64,
        "ClientCode": pl.Utf8,
        "QuantityRequested": pl.Utf8,
        "SegmentProductRatio": pl.Float64,
        "Cost": pl.Float64,
        "LoggedUTC": pl.Datetime,
    },
    "TreatmentProductInputParameterPivot": {
        "TreatmentProductSequence": pl.Int64,
        "TreatmentID": pl.Int64,
        "FareClass": pl.Utf8,
        "JourneyFareClassOfServiceList": pl.Utf8,  # -- Not Found Dummy Column
        "FareClassID": pl.Int32,  # -- Not Found Dummy Column
        "ProbabilityMatch": pl.Float64,  # -- Not Found Dummy Column
        "BookingWeekday": pl.Utf8,  # -- Not Found Dummy Column
        "output_label1ID": pl.Int32,  # -- Not Found Dummy Column
        "BookingWeekdayID": pl.Int32,  # -- Not Found Dummy Column
        "LabelMatch": pl.Utf8,  # -- Not Found Dummy Column
        "LineOfBusiness": pl.Utf8,  # -- Not Found Dummy Column
        "output_label1": pl.Utf8,  # -- Not Found Dummy Column
        "output_probability1": pl.Float64,  # -- Not Found Dummy Column
        "output_probability2": pl.Float64,  # -- Not Found Dummy Column
        "LoggedUTC": pl.Datetime
    },
    "TreatmentDistinctRanked": {
        "MinTreatmentID": pl.Int64,
        "SamplingRandomNumber": pl.Float64,
        "MinLoggedUTC": pl.Datetime,
        "MaxLoggedUTC": pl.Datetime,
        "MaxExternalBookingID": pl.Utf8,
        "ExternalBookingID": pl.Utf8,
        "Stage": pl.Int32,
        "TripReturnTravelDate": pl.Datetime,
        "TripFirstTravelDate": pl.Datetime,
        "PaxType": pl.Utf8,
        "ChannelID": pl.Utf8,
        "RoleName": pl.Utf8,
        "TripOriginLocationCode": pl.Utf8,
        "TripDestinationLocationCode": pl.Utf8,
        "NumberOfPassengers": pl.Int32,
        "MinTotalItineraryPrice": pl.Float64,
        "FareClass": pl.Utf8,
        "TripType": pl.Int32,
        "TreatmentRank": pl.Int32,
        "AgentID": pl.Int32,
        "CustomerHomeCity": pl.Utf8,
        "MarketingCarrierCode": pl.Utf8,  # Doubled up
        "OperatingCarrierCode": pl.Utf8,
        "ResidentCountryCode": pl.Utf8,
        "SegmentDestinationCountryCode": pl.Utf8,
        "SegmentOriginLocationCodeTimeZoneOffsetMinutes": pl.Int32,
        "SegmentTravelTime": pl.Datetime,
        "ServiceBundleCode": pl.Utf8,
        "TotalFareWithoutFeeAndTax": pl.Float64,
        "TravelBooked": pl.Boolean,
        "TripDestinationCountryCode": pl.Utf8,
        "TripOriginCountryCode": pl.Utf8,
        # -- "IPAddress": pl.Utf8, -- implicitly commented
        "IsInServiceBundle": pl.Utf8,
        "JourneyTravelTime": pl.Utf8,
        "SegmentDestinationLocationCode": pl.Utf8,
        "SegmentFirstTravelDate": pl.Datetime,
        "SegmentOriginCountryCode": pl.Utf8,
        "SegmentOriginLocationCode": pl.Utf8,
        "TotalFare": pl.Float64,
        "TripOriginLocationCodeTimeZoneOffsetMinutes": pl.Int32,
        "ContryDestinationLocationCode": pl.Utf8,  # -- Not Found Dummy Column and potential typo
        "ContryOriginLocationCode": pl.Utf8,  # -- Not Found Dummy Column and potential typo
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
        "JourneyDestinationLocationCode": pl.Utf8,
        "JourneyDestinationCountryCode": pl.Utf8,
        "JourneyOriginCountryCode": pl.Utf8,
        "JourneyOriginLocationCode": pl.Utf8,
        # -- "PassengerCustomerNumber": pl.Utf8,
        "JourneyMarketingCarrierCodeList": pl.Utf8,
        "JourneyOperatingCarrierCodeList": pl.Utf8,
        "JourneyOperatingFlightNumberList": pl.Utf8,
        "JourneyMarketingFlightNumberList": pl.Utf8,
        # "RecordLocator": pl.Utf8, -- explicitly commented out
        "JourneyFlightType": pl.Utf8,
        "WebSessionID": pl.Utf8,  # -- Not Found Dummy Column
        "JourneySoldLegByTravelClassList": pl.Utf8,  # -- Not Found Dummy Column
        "JourneyConnectingStationsList": pl.Utf8,  # -- Not Found Dummy Column
        "MMB": pl.Utf8,  # -- Not Found Dummy Column
        "SessionStartDateTime": pl.Utf8,
        "Workflow": pl.Utf8,
        "ApplyCjDiscount": pl.Utf8,  # -- Not Found Dummy Column
        "ClientName": pl.Utf8,  # -- Not Found Dummy Column
        "SegmentEquipmentType": pl.Utf8,
        "SegmentEquipmentTypeSuffix": pl.Utf8,
        "TotalFareAdjustment": pl.Utf8,
        "TotalFarePoints": pl.Utf8,
        "TotalFarePointsAdjustment": pl.Utf8,
        "TotalOriginalFare": pl.Float64,
        "SourceOrganizationCode": pl.Utf8 # -- Not Found Dummy Column
    }
}

for i in treatment_files:
    directory_name = f"./TreatmentExport/{i}"
    directory_path = Path(directory_name)

    target_files = []
    # Extract Base File and Parts if present
    for file in directory_path.iterdir():
        #print(file.is_file() and "-09-" in file.name, ".zip" in file.name, int(file.name.split("-")[-1].strip(".zip")) >= 18) # and int(file.name.split("-")[-1].strip(".zip")) <= 23:)
        if file.is_file() and "-09-" in file.name and ".zip" in file.name and int(file.name.split("-")[-1].strip(".zip")) >= 24: # and int(file.name.split("-")[-1].strip(".zip")) <= 23:
            target_files.append(file.name)

    for target_file in target_files:
        zip_file = target_file
        csv_file = target_file.replace(".zip", ".csv")

        target = f"{directory_name}/{zip_file}"
        target_csv = target.replace(".zip", ".csv")

        print(target_csv)

        with zipfile.ZipFile(target, 'r') as zip_ref:
            zip_ref.extractall(directory_name)

        memory = pl.read_csv(target_csv, separator=";", schema=data_types[i])

        # with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as zipf:
        #     zipf.write(target_csv, os.path.basename(target_csv))

        if os.path.exists(target_csv):
            os.remove(target_csv)

        time.sleep(30.0)

