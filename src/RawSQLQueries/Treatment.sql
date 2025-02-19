SELECT
[TreatmentID]
      ,[ClientCode]
      ,[SamplingRandomNumber]
      ,[LoggedUTC]
      ,[SourceCode]
      ,[TripFirstTravelDate]
      ,[PaxType]
      ,[ChannelID]
      ,[RoleName]
      ,[TripOriginLocationCode]
      ,[TripDestinationLocationCode]
      ,[ExternalBookingID]
      ,[NumberOfPassengers]
      ,[TotalItineraryPrice]
      ,[FareClass]
      ,[TripType]
      ,[Stage]
      ,[ValueType]
      ,[RequestedCurrency]
      ,[SessionStartDateTime]
  FROM [ANAJQDW01].[AnalyticsDW].[Treatment] t
  WHERE t.[TreatmentID] >= %s and t.[TreatmentID] <= %s

