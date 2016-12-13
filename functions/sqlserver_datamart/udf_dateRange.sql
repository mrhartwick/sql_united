-- table funtion to create a date range (datetime output) based on parameters given
-- range can be incremented by most DATEADD dateparts between millisecond and year

ALTER FUNCTION [dbo].[udf_dateRange]
(
      @Increment             CHAR(1),
      @stDate                DATETIME,
      @edDate                DATETIME
)
RETURNS
@SelectedRange    TABLE
(dateList DATETIME)
AS
BEGIN
      ;WITH cteRange (dateRange) AS (
            SELECT @stDate
            UNION ALL
            SELECT
                  CASE
                        WHEN @Increment = 'ms' THEN DATEADD(ms, 1, dateRange)   -- millisecond
                        WHEN @Increment = 'ss' THEN DATEADD(ss, 1, dateRange)   -- second
                        WHEN @Increment = 'mi' THEN DATEADD(mi, 1, dateRange)   -- minute
                        WHEN @Increment = 'hh' THEN DATEADD(hh, 1, dateRange)   -- hour
                        WHEN @Increment = 'd'  THEN DATEADD(dd, 1, dateRange)   -- day
                        WHEN @Increment = 'w'  THEN DATEADD(ww, 1, dateRange)   -- week
                        WHEN @Increment = 'm'  THEN DATEADD(mm, 1, dateRange)   -- month
                        WHEN @Increment = 'q'  THEN DATEADD(qq, 1, dateRange)   -- quarter
                        WHEN @Increment = 'yy' THEN DATEADD(yy, 1, dateRange)   -- year
                  END
            FROM cteRange
            WHERE dateRange <=
                  CASE
                        WHEN @Increment = 'ms' THEN DATEADD(ms, -1, @edDate)   -- millisecond
                        WHEN @Increment = 'ss' THEN DATEADD(ss, -1, @edDate)   -- second
                        WHEN @Increment = 'mi' THEN DATEADD(mi, -1, @edDate)   -- minute
                        WHEN @Increment = 'hh' THEN DATEADD(hh, -1, @edDate)   -- hour
                        WHEN @Increment = 'd'  THEN DATEADD(dd, -1, @edDate)   -- day
                        WHEN @Increment = 'w'  THEN DATEADD(ww, -1, @edDate)   -- week
                        WHEN @Increment = 'm'  THEN DATEADD(mm, -1, @edDate)   -- month
                        WHEN @Increment = 'q'  THEN DATEADD(qq, -1, @edDate)   -- month
                        WHEN @Increment = 'yy' THEN DATEADD(yy, -1, @edDate)   -- year
                  END)

      INSERT INTO @SelectedRange (dateList)
      SELECT dateRange
      FROM cteRange
      OPTION (MAXRECURSION 3660);
      RETURN
END
GO