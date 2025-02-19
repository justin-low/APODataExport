SELECT
		[MinTreatmentID]
      ,[SamplingRandomNumber]
      ,[MinLoggedUTC]
      ,[MaxLoggedUTC]
      ,[MaxExternalBookingID]
      ,[ExternalBookingID]
      ,[Stage]
      ,[TripReturnTravelDate]
      ,[TripFirstTravelDate]
      ,[PaxType]
      ,[ChannelID]
      ,[RoleName]
      ,[TripOriginLocationCode]
      ,[TripDestinationLocationCode]
      ,[NumberOfPassengers]
      ,[MinTotalItineraryPrice]
      ,[FareClass]
      ,[TripType]
      ,[TreatmentRank]
      ,[AgentID]
	  ,[CustomerHomeCity]
	  ,[MarketingCarrierCode] --- Double up on this
	  ,[OperatingCarrierCode]
	  ,[ResidentCountryCode]
	  ,[SegmentDestinationCountryCode]
	  ,[SegmentOriginLocationCodeTimeZoneOffsetMinutes]
	  ,[SegmentTravelTime]
	  ,[ServiceBundleCode]
	  ,[TotalFareWithoutFeeAndTax]
	  ,[TravelBooked]
	  ,[TripDestinationCountryCode]
	  ,[TripOriginCountryCode]
	  --,[IPAddress] -- explicitly commented out - so not included
	  ,[IsInServiceBundle]
	  ,[JourneyTravelTime]
	  ,[SegmentDestinationLocationCode]
	  ,[SegmentFirstTravelDate]
	  ,[SegmentOriginCountryCode]
	  ,[SegmentOriginLocationCode]
	  ,[TotalFare]
	  ,[TripOriginLocationCodeTimeZoneOffsetMinutes]
	  ,NULL AS [ContryDestinationLocationCode] -- Not Found Dummy Column and potential typo
	  ,NULL AS [ContryOriginLocationCode] -- Not Found Dummy Column and potential typo
	  ,[FirstPassengerHomeCity]
	  ,[TestName]
	  --,[PassengerProgramNumber] explicitly commented out
	  ,[OverrideSamplingRandomNumber]
	  --,[PassengerProgramCode] explicitly commented out
	  ,[PromotionCode]
	  ,NULL AS [CustomDiscountCode] -- Not Found Dummy Column
	  ,NULL AS [AssignableSeatsCount] -- Not Found Dummy Column
	  ,[SegmentEquipmentSalesConfiguration]
	  ,NULL AS [CjDiscountCode] -- Not Found Dummy Column
	  ,[LoyaltyFilter]
	  ,[PricingDate]
	  ,[JourneyDestinationLocationCode]
	  ,[JourneyDestinationCountryCode]
	  ,[JourneyOriginCountryCode]
	  ,[JourneyOriginLocationCode]
	  --,[PassengerCustomerNumber] -- explicitly commented out
	  ,[JourneyMarketingCarrierCodeList]
	  ,[JourneyOperatingCarrierCodeList]
	  ,[JourneyOperatingFlightNumberList]
	  ,[JourneyMarketingFlightNumberList]
	  --,[RecordLocator] -- explicitly commented out
	  ,[JourneyFlightType]
	  ,NULL AS [WebSessionID] -- Not Found Dummy Column I think I will know where to recover this
	  ,NULL AS [JourneySoldLegByTravelClassList] -- Not Found Dummy Column
	  ,NULL AS [JourneyConnectingStationsList] -- Not Found Dummy Column
	  ,NULL AS [MMB] -- Not Found Dummy Column
	  ,[SessionStartDateTime]
	  ,[Workflow]
	  ,NULL AS [ApplyCjDiscount] -- Not Found Dummy Column
	  ,NULL AS [ClientName] -- Not Found Dummy Column
	  ,[SegmentEquipmentType]
	  ,[SegmentEquipmentTypeSuffix]
	  ,[TotalFareAdjustment]
	  ,[TotalFarePoints]
	  ,[TotalFarePointsAdjustment]
	  ,[TotalOriginalFare]
	  ,NULL AS [SourceOrganizationCode] -- Not Found Dummy Column not found But I think I will be able to recover this
  FROM [ANAJQDW01].[AnalyticsDW].[TreatmentDistinctRanked] tdr
  WHERE tdr.[MinTreatmentID] >= %s and tdr.[MinTreatmentID] <= %s