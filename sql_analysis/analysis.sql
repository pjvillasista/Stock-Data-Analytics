-- V1
WITH stock_returns as(
    SELECT
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        -- Determine if the current adj_close is the maximum up to this date
        ("Adj Close" = MAX("Adj Close") OVER (
            ORDER BY "Date" 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS all_time_high,
        -- Calculate the return after 6 months (126 trading days)
        ((LEAD("Adj Close", 126) OVER (
            ORDER BY "Date") - "Adj Close") / "Adj Close") AS return_6_months,
        -- Calculate the return after 1 year (252 trading days)
        ((LEAD("Adj Close", 252) OVER (
            ORDER BY "Date") - "Adj Close") / "Adj Close") AS return_1_year,
        -- Calculate the return after 2 years (504 trading days)
        ((LEAD("Adj Close", 504) OVER (
            ORDER BY "Date") - "Adj Close") / "Adj Close") AS return_2_years,
        -- Calculate the return after 5 years (1260 trading days)
        ((LEAD("Adj Close", 1260) OVER (
            ORDER BY "Date") - "Adj Close") / "Adj Close") AS return_5_years
    FROM public.spy_data
)
-- Query to analyze average returns on all-time high days vs. other days
SELECT 
    'All Time Highs' AS category,
    AVG(return_6_months) AS avg_return_6_months,
    AVG(return_1_year) AS avg_return_1_year,
    AVG(return_2_years) AS avg_return_2_years,
    AVG(return_5_years) AS avg_return_5_years
FROM stock_returns
WHERE all_time_high = true

UNION ALL

SELECT 
    'Random Days' AS category,
    AVG(return_6_months) AS avg_return_6_months,
    AVG(return_1_year) AS avg_return_1_year,
    AVG(return_2_years) AS avg_return_2_years,
    AVG(return_5_years) AS avg_return_5_years
FROM stock_returns
WHERE all_time_high = false;


-- V2
WITH ReturnData AS(
    SELECT
        "Date",
        "Adj Close",
        LEAD("Adj Close", 126) OVER (ORDER BY "Date") / "Adj Close" - 1 AS Return_6_Months,
        LEAD("Adj Close", 252) OVER (ORDER BY "Date") / "Adj Close" - 1 AS Return_1_Year,
        LEAD("Adj Close", 504) OVER (ORDER BY "Date") / "Adj Close" - 1 AS Return_2_Years,
        LEAD("Adj Close", 756) OVER (ORDER BY "Date") / "Adj Close" - 1 AS Return_3_Years,
        LEAD("Adj Close", 1260) OVER (ORDER BY "Date") / "Adj Close" - 1 AS Return_5_Years
    FROM spy_data
),
RankedData AS (
    SELECT
        "Date",
        "Adj Close",
        MAX("Adj Close") OVER (ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Max_To_Date
    FROM spy_data
),
CombinedData AS (
    SELECT
        RD."Date",
        RD."Adj Close",
        CASE WHEN RD."Adj Close" = MD.Max_To_Date THEN 1 ELSE 0 END AS Is_All_Time_High,
        RD.Return_6_Months,
        RD.Return_1_Year,
        RD.Return_2_Years,
        RD.Return_3_Years,
        RD.Return_5_Years
    FROM ReturnData RD
    JOIN (
        SELECT "Date", MAX("Adj Close") OVER (ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Max_To_Date
        FROM spy_data
    ) MD ON RD."Date" = MD."Date"
)
SELECT
    CASE WHEN Is_All_Time_High = 1 THEN 'All-Time Highs' ELSE 'Other Days' END AS Day_Type,
    AVG(Return_6_Months) AS Avg_Return_6_Months,
    AVG(Return_1_Year) AS Avg_Return_1_Year,
    AVG(Return_2_Years) AS Avg_Return_2_Years,
    AVG(Return_3_Years) AS Avg_Return_3_Years,
    AVG(Return_5_Years) AS Avg_Return_5_Years
FROM CombinedData
GROUP BY Is_All_Time_High;
