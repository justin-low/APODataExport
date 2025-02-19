SELECT
       tpipp.[TreatmentProductSequence]
	  ,tpipp.[TreatmentID]
      ,tpipp.[FareClass]
	  ,NULL AS [JourneyFareClassOfServiceList] -- Not Found Dummy Column
	  ,NULL AS [FareClassID] -- Not Found Dummy Column
	  ,NULL AS [ProbabilityMatch] -- Not Found Dummy Column
	  ,NULL AS [BookingWeekday] -- Not Found Dummy Column
	  ,NULL AS [output_labellID] -- Not Found Dummy Column
	  ,NULL AS [BookingWeekdayID] -- Not Found Dummy Column
	  ,NULL AS [LabelMatch] -- Not Found Dummy Column
	  ,NULL AS [LineOfBusiness] -- Not Found Dummy Column
	  ,NULL AS [output_label1] -- Not Found Dummy Column
	  ,NULL AS [output_probability1] -- Not Found Dummy Column
	  ,NULL AS [output_probability2] -- Not Found Dummy Column
	  ,t.[LoggedUTC]
  FROM [ANAJQDW01].[AnalyticsDW].[TreatmentProductInputParameterPivot] tpipp
  INNER JOIN [ANAJQDW01].[AnalyticsDW].[Treatment] t ON t.[TreatmentID] = tpipp.[TreatmentID]
  WHERE t.TreatmentID >= %s and t.TreatmentID <= %s

