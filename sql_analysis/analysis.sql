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