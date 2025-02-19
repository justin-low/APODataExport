SELECT
		tp.[TreatmentProductSequence]
	  , tp.[ProductID]
	  , tp.[Amount]
	  , tp.[OriginalPrice]
	  , tp.[CurrencyCode]
      , tp.[QuantityAvailable]
	  , tp.[CalculatedFrom]
	  , tp.[GroupCode]
	  , tp.[TreatmentOrdinal]
	  , tp.[OfferCode]
	  , tp.[OptimizedPoints]
	  , tp.[TreatmentID]
	  , tp.[ClientCode]
      , tp.[QuantityRequested]
      , tp.[SegmentProductRatio]
      , tp.[Cost]
      , t.[LoggedUTC]
  FROM [ANAJQDW01].[AnalyticsDW].[TreatmentProduct] tp
 INNER JOIN [ANAJQDW01].[AnalyticsDW].[Treatment] t ON t.TreatmentID = tp.TreatmentID
  WHERE t.TreatmentID >= %s and t.TreatmentID <= %s
