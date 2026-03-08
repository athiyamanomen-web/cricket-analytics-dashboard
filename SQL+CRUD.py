import streamlit as st
import mysql.connector
import pandas as pd

SERIES_ID = 11253

queries = {

"1. Players who represent India": """
SELECT
    name AS `Full Name`,
    role AS `Playing Role`,
    bat_style AS `Batting Style`,
    bowl_style AS `Bowling Style`
FROM players
WHERE country = 'India'
ORDER BY name;
""",

"2. Matches played in the last few days": """
SELECT
    match_desc AS `Match Description`,
    team1 AS `Team 1`,
    team2 AS `Team 2`,
    CONCAT(venue, ', ', city) AS `Venue & City`,
    match_date_utc AS `Match Date`
FROM recent_matches
ORDER BY match_date_utc DESC;
""",

"3. Top 10 highest ODI run scorers": """
SELECT
    batter AS `Player`,
    runs AS `Runs`,
    avg AS `Batting Average`
FROM top_batting_stats_runs
WHERE format = 'ODI'
ORDER BY runs DESC
LIMIT 10;
""",

"4. Cricket Venues (Capacity > 25000 ordered descending)": """
SELECT
    venue_name AS Venue,
    city AS City,
    country AS Country,
    capacity AS Capacity
FROM venues
WHERE capacity > 25000
ORDER BY capacity DESC
LIMIT 10;
""",

"5. Series Points Table (Wins Highest First)": None,

"5. b) Matches won by each team": """
SELECT
    winner AS `Team`,
    COUNT(*) AS `Total Wins`
FROM series_matches
WHERE state = 'complete'
  AND winner IS NOT NULL
  AND winner <> ''
GROUP BY winner
ORDER BY `Total Wins` DESC;
""",

"6. Count players by playing role": """
SELECT
    role AS `Playing Role`,
    COUNT(*) AS `Number of Players`
FROM players
WHERE country = 'India'
GROUP BY role
ORDER BY `Number of Players` DESC;
""",

"7. Highest individual batting score in each format (Test/ODI/T20I)": """
SELECT
    format AS `Format`,
    MAX(highest) AS `Highest Individual Score`
FROM player_batting_career_summary
WHERE format IN ('Test','ODI','T20')
GROUP BY format
ORDER BY FIELD(format,'Test','ODI','T20');
""",

"8. Cricket Series Information (2024)": """
SELECT
    series_name AS `Series Name`,
    host_countries AS `Host Countries`,
    match_type AS `Match Type`,
    series_start_date AS `Start Date`,
    total_matches_planned AS `Total Matches`
FROM series_info
ORDER BY series_start_date DESC;
""",

"9. All-rounders with more than 1000 runs and 50 wickets": """
SELECT
    p.name AS `Player Name`,
    b.format AS `Cricket Format`,
    b.runs AS `Total Runs`,
    bw.wickets AS `Total Wickets`
FROM player_batting_career_summary b
JOIN player_bowling_career_summary bw
    ON b.player_id = bw.player_id
   AND b.format = bw.format
JOIN players p
    ON b.player_id = p.player_id
WHERE LOWER(p.role) LIKE '%all%'
  AND LOWER(p.role) LIKE '%round%'
  AND b.runs > 1000
  AND bw.wickets > 50
ORDER BY b.format, b.runs DESC, bw.wickets DESC;
""",

"10. Last 20 completed matches (Recent Matches First)": """
SELECT
    match_desc AS `Match Description`,
    team1 AS `Team 1`,
    team2 AS `Team 2`,
    SUBSTRING_INDEX(status, ' won by ', 1) AS `Winning Team`,
    SUBSTRING_INDEX(SUBSTRING_INDEX(status, ' won by ', -1), ' ', 1) AS `Victory Margin`,
    CASE
        WHEN LOWER(status) LIKE '%wkt%' OR LOWER(status) LIKE '%wicket%' THEN 'Wickets'
        WHEN LOWER(status) LIKE '%run%' THEN 'Runs'
        ELSE 'Other'
    END AS `Victory Type`,
    venue AS `Venue Name`
FROM recent_matches
WHERE state = 'Complete'
  AND status LIKE '%won by%'
ORDER BY match_date_utc DESC
LIMIT 20;
""",

"11. Compare player performance across formats": """
SELECT
    p.name AS `Player Name`,
    SUM(CASE WHEN b.format = 'Test' THEN b.runs ELSE 0 END) AS `Test Runs`,
    SUM(CASE WHEN b.format = 'ODI' THEN b.runs ELSE 0 END) AS `ODI Runs`,
    SUM(CASE WHEN b.format = 'T20' THEN b.runs ELSE 0 END) AS `T20I Runs`,
    ROUND(
        SUM(b.runs) / NULLIF(SUM(b.innings - b.not_out), 0),
        2
    ) AS `Overall Batting Average`
FROM player_batting_career_summary b
JOIN players p
    ON b.player_id = p.player_id
WHERE b.format IN ('Test', 'ODI', 'T20')
GROUP BY p.player_id, p.name
HAVING
    SUM(CASE WHEN b.format = 'Test' AND b.innings > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN b.format = 'ODI'  AND b.innings > 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN b.format = 'T20'  AND b.innings > 0 THEN 1 ELSE 0 END) >= 2
ORDER BY `Overall Batting Average` DESC;
""",

"12. International Team Performance (Home & Away)": """
WITH match_data AS (
    SELECT 
        team1_name AS team,
        TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(series_name,'tour of',-1),',',1)) AS host_team,
        CASE WHEN status LIKE '% won by %' THEN TRIM(SUBSTRING_INDEX(status,' won by ',1)) ELSE NULL END AS winner,
        state
    FROM series_matches
    WHERE series_name LIKE '%tour of%'

    UNION ALL

    SELECT 
        team2_name AS team,
        TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(series_name,'tour of',-1),',',1)),
        CASE WHEN status LIKE '% won by %' THEN TRIM(SUBSTRING_INDEX(status,' won by ',1)) ELSE NULL END,
        state
    FROM series_matches
    WHERE series_name LIKE '%tour of%'
),

classified_matches AS (
    SELECT
        team,
        CASE WHEN team = host_team THEN 'Home' ELSE 'Away' END AS location,
        CASE WHEN state='complete' AND team = winner THEN 1 ELSE 0 END AS win
    FROM match_data
)

SELECT
    team,
    SUM(CASE WHEN location='Home' THEN 1 ELSE 0 END) AS home_matches,
    SUM(CASE WHEN location='Away' THEN 1 ELSE 0 END) AS away_matches,
    SUM(CASE WHEN location='Home' THEN win ELSE 0 END) AS home_wins,
    SUM(CASE WHEN location='Away' THEN win ELSE 0 END) AS away_wins,
    ROUND(
        SUM(CASE WHEN location='Home' THEN win ELSE 0 END)*100 /
        NULLIF(SUM(CASE WHEN location='Home' THEN 1 ELSE 0 END),0),2
    ) AS home_win_percentage,
    ROUND(
        SUM(CASE WHEN location='Away' THEN win ELSE 0 END)*100 /
        NULLIF(SUM(CASE WHEN location='Away' THEN 1 ELSE 0 END),0),2
    ) AS away_win_percentage
FROM classified_matches
GROUP BY team
ORDER BY home_wins DESC, away_wins DESC, team;
""",

"13.A)Identify batting partnerships with 100 or more runs in the same innings.": """
SELECT
    sm.series_name,
    sm.match_desc,
    sm.match_format,
    p.team_name,
    p.innings_id,
    p.bat1_name AS batsman_1,
    p.bat2_name AS batsman_2,
    p.total_runs AS partnership_runs
FROM partnerships p
JOIN series_matches sm
    ON p.match_id = sm.match_id
WHERE p.total_runs >= 100
ORDER BY p.total_runs DESC, sm.start_date DESC
""",

"13.B)Identify batting partnerships with 100 or more runs in the same innings by(2 Consecutive Batsmen).": """
WITH batting_order AS (
    SELECT
        id,
        match_id,
        innings_id,
        batsman_name,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, innings_id
            ORDER BY id
        ) AS bat_position
    FROM batting_scorecard
),
partnership_with_positions AS (
    SELECT
        p.match_id,
        p.innings_id,
        p.innings_no,
        p.team_name,
        p.bat1_name,
        p.bat2_name,
        p.total_runs,
        bo1.bat_position AS bat1_position,
        bo2.bat_position AS bat2_position
    FROM partnerships p
    JOIN batting_order bo1
        ON p.match_id = bo1.match_id
       AND p.innings_id = bo1.innings_id
       AND p.bat1_name = bo1.batsman_name
    JOIN batting_order bo2
        ON p.match_id = bo2.match_id
       AND p.innings_id = bo2.innings_id
       AND p.bat2_name = bo2.batsman_name
)
SELECT
    sm.series_name,
    sm.match_desc,
    pwp.innings_no,
    pwp.bat1_name AS batsman_1,
    pwp.bat2_name AS batsman_2,
    pwp.total_runs AS partnership_runs
FROM partnership_with_positions pwp
JOIN series_matches sm
    ON pwp.match_id = sm.match_id
WHERE ABS(pwp.bat1_position - pwp.bat2_position) = 1
  AND pwp.total_runs >= 100
ORDER BY pwp.total_runs DESC;
""",

"14.Bowling performance at different venues": """
SELECT
    bw.bowler_name,
    sm.venue_name,
    COUNT(DISTINCT bw.match_id) AS matches_played,
    ROUND(AVG(bw.runs_conceded * 6.0 / NULLIF(bw.balls, 0)), 2) AS avg_economy_rate,
    SUM(bw.wickets) AS total_wickets
FROM bowling_scorecard_v2 bw
JOIN series_matches_v2 sm
    ON sm.match_id = bw.match_id
WHERE bw.balls >= 24
GROUP BY bw.bowler_name, sm.venue_name
HAVING COUNT(DISTINCT bw.match_id) >= 3
ORDER BY avg_economy_rate ASC, total_wickets DESC, matches_played DESC;
""",

"15. Players who play exceptionally well in close matches": """  
SELECT
    b.player_name AS Player,
    ROUND(AVG(b.runs), 2) AS avg_runs_in_close_matches,
    COUNT(DISTINCT b.match_id) AS close_matches_played,
    SUM(CASE WHEN b.batting_team_won = 1 THEN 1 ELSE 0 END) AS close_matches_team_won,
    GROUP_CONCAT(
        DISTINCT CONCAT(sm.match_desc, ' - ', sm.status)
        ORDER BY sm.start_date
        SEPARATOR ' | '
    ) AS close_match_statuses
FROM player_match_batting_view b
JOIN series_matches sm
    ON b.match_id = sm.match_id
WHERE
    (
        (sm.win_margin_runs IS NOT NULL AND sm.win_margin_runs < 50)
        OR
        (sm.win_margin_wickets IS NOT NULL AND sm.win_margin_wickets < 5)
    )
GROUP BY b.player_name
ORDER BY avg_runs_in_close_matches DESC, close_matches_played DESC;
""",

"16. Track batting performance changes over different years": """
SELECT
    b.batsman_name AS `Player`,
    YEAR(s.start_date) AS `Year`,
    ROUND(SUM(b.runs) / COUNT(DISTINCT b.match_id), 2) AS `Average Runs Per Match`,
    ROUND(SUM(b.runs) * 100.0 / NULLIF(SUM(b.balls), 0), 2) AS `Average Strike Rate`,
    COUNT(DISTINCT b.match_id) AS `Matches Played`
FROM batting_scorecard_v2 b
JOIN series_matches_v2 s
    ON b.match_id = s.match_id
WHERE YEAR(s.start_date) >= 2020
GROUP BY
    b.batsman_name,
    YEAR(s.start_date)
HAVING COUNT(DISTINCT b.match_id) >= 5
ORDER BY
    `Year` DESC,
    `Average Runs Per Match` DESC;
""",

"17. Toss advantage by toss decision": """
WITH toss_base AS (
    SELECT
        ml.match_id,
        ml.toss_winner_name,
        ml.toss_decision,
        ml.team1_id,
        ml.team1_name,
        ml.team2_id,
        ml.team2_name,
        ml.winning_team_id,
        CASE
            WHEN UPPER(TRIM(ml.toss_decision)) IN ('FIELD', 'BOWL', 'BOWLING') THEN 'Bowl First'
            ELSE 'Bat First'
        END AS toss_decision_group,
        CASE
            WHEN UPPER(TRIM(ml.toss_winner_name)) = UPPER(TRIM(ml.team1_name)) THEN ml.team1_id
            WHEN UPPER(TRIM(ml.toss_winner_name)) = UPPER(TRIM(ml.team2_name)) THEN ml.team2_id
            ELSE NULL
        END AS toss_winner_id
    FROM matches_leanback ml
    WHERE ml.toss_winner_name IS NOT NULL
      AND ml.winning_team_id IS NOT NULL
)
SELECT
    toss_decision_group,
    COUNT(*) AS total_matches,
    SUM(CASE WHEN toss_winner_id = winning_team_id THEN 1 ELSE 0 END) AS toss_winner_wins,
    ROUND(
        100.0 * SUM(CASE WHEN toss_winner_id = winning_team_id THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS win_percentage
FROM toss_base
WHERE toss_winner_id IS NOT NULL
GROUP BY toss_decision_group
ORDER BY win_percentage DESC;
""",

"18. Most economical bowlers in ODI and T20": """
SELECT
    bw.bowler_name,
    COUNT(DISTINCT bw.match_id) AS matches_played,
    ROUND(SUM(bw.balls) / 6.0 / COUNT(DISTINCT bw.match_id), 2) AS avg_overs_per_match,
    SUM(bw.wickets) AS total_wickets,
    ROUND(SUM(bw.runs_conceded) * 6.0 / NULLIF(SUM(bw.balls), 0), 2) AS overall_economy
FROM bowling_scorecard_v2 bw
JOIN series_matches_v2 sm
    ON sm.match_id = bw.match_id
WHERE sm.match_format IN ('ODI', 'T20', 'T20I')
GROUP BY bw.bowler_name
HAVING COUNT(DISTINCT bw.match_id) >= 10
   AND (SUM(bw.balls) / 6.0 / COUNT(DISTINCT bw.match_id)) >= 2
ORDER BY overall_economy ASC, total_wickets DESC;
""",

"19. Most consistent batsmen since 2022": """
SELECT
    b.batsman_name,
    COUNT(*) AS innings_played,
    ROUND(AVG(b.runs), 2) AS avg_runs,
    ROUND(STDDEV(b.runs), 2) AS runs_stddev
FROM batting_scorecard_v2 b
JOIN series_matches_v2 sm
    ON sm.match_id = b.match_id
WHERE sm.start_date >= '2022-01-01'
  AND b.balls >= 10
GROUP BY b.batsman_name
HAVING COUNT(*) >= 5
ORDER BY runs_stddev ASC, avg_runs DESC;
""",

"20. Matches played and batting average by format": """
SELECT
    b.batsman_name,
    COUNT(DISTINCT CASE WHEN sm.match_format = 'Test' THEN b.match_id END) AS test_matches,
    ROUND(SUM(CASE WHEN sm.match_format = 'Test' THEN b.runs ELSE 0 END) /
          NULLIF(SUM(CASE WHEN sm.match_format = 'Test'
                           AND LOWER(TRIM(COALESCE(b.out_desc,''))) <> 'not out'
                           AND TRIM(COALESCE(b.out_desc,'')) <> ''
                      THEN 1 ELSE 0 END), 0), 2) AS test_avg,

    COUNT(DISTINCT CASE WHEN sm.match_format = 'ODI' THEN b.match_id END) AS odi_matches,
    ROUND(SUM(CASE WHEN sm.match_format = 'ODI' THEN b.runs ELSE 0 END) /
          NULLIF(SUM(CASE WHEN sm.match_format = 'ODI'
                           AND LOWER(TRIM(COALESCE(b.out_desc,''))) <> 'not out'
                           AND TRIM(COALESCE(b.out_desc,'')) <> ''
                      THEN 1 ELSE 0 END), 0), 2) AS odi_avg,

    COUNT(DISTINCT CASE WHEN sm.match_format IN ('T20','T20I') THEN b.match_id END) AS t20_matches,
    ROUND(SUM(CASE WHEN sm.match_format IN ('T20','T20I') THEN b.runs ELSE 0 END) /
          NULLIF(SUM(CASE WHEN sm.match_format IN ('T20','T20I')
                           AND LOWER(TRIM(COALESCE(b.out_desc,''))) <> 'not out'
                           AND TRIM(COALESCE(b.out_desc,'')) <> ''
                      THEN 1 ELSE 0 END), 0), 2) AS t20_avg,

    COUNT(DISTINCT b.match_id) AS total_matches
FROM batting_scorecard_v2 b
JOIN series_matches_v2 sm ON sm.match_id = b.match_id
WHERE sm.match_format IN ('Test','ODI','T20','T20I')
GROUP BY b.batsman_name
HAVING COUNT(DISTINCT b.match_id) >= 20
ORDER BY total_matches DESC, b.batsman_name;
""",

"21. Comprehensive player ranking by format": """
WITH stats AS (
    SELECT
        sm.match_format,
        b.batsman_name AS player_name,
        COUNT(DISTINCT b.match_id) AS matches_played,
        SUM(b.runs) AS runs_scored,
        ROUND(SUM(b.runs) / NULLIF(SUM(CASE
            WHEN TRIM(COALESCE(b.out_desc,'')) <> ''
             AND LOWER(TRIM(COALESCE(b.out_desc,''))) <> 'not out'
            THEN 1 ELSE 0 END), 0), 2) AS batting_average,
        ROUND(SUM(b.runs) * 100.0 / NULLIF(SUM(b.balls), 0), 2) AS strike_rate,
        0 AS wickets_taken,
        50 AS bowling_average,
        6 AS economy_rate
    FROM batting_scorecard_v2 b
    JOIN series_matches_v2 sm ON sm.match_id = b.match_id
    WHERE sm.match_format IN ('Test','ODI','T20','T20I')
    GROUP BY sm.match_format, b.batsman_name
    HAVING COUNT(DISTINCT b.match_id) >= 10

    UNION ALL

    SELECT
        sm.match_format,
        bw.bowler_name AS player_name,
        COUNT(DISTINCT bw.match_id) AS matches_played,
        0 AS runs_scored,
        0 AS batting_average,
        0 AS strike_rate,
        SUM(bw.wickets) AS wickets_taken,
        ROUND(SUM(bw.runs_conceded) / NULLIF(SUM(bw.wickets), 0), 2) AS bowling_average,
        ROUND(SUM(bw.runs_conceded) * 6.0 / NULLIF(SUM(bw.balls), 0), 2) AS economy_rate
    FROM bowling_scorecard_v2 bw
    JOIN series_matches_v2 sm ON sm.match_id = bw.match_id
    WHERE sm.match_format IN ('Test','ODI','T20','T20I')
    GROUP BY sm.match_format, bw.bowler_name
    HAVING COUNT(DISTINCT bw.match_id) >= 10
),
combined AS (
    SELECT
        match_format,
        player_name,
        SUM(runs_scored) AS runs_scored,
        MAX(batting_average) AS batting_average,
        MAX(strike_rate) AS strike_rate,
        SUM(wickets_taken) AS wickets_taken,
        MIN(bowling_average) AS bowling_average,
        MIN(economy_rate) AS economy_rate
    FROM stats
    GROUP BY match_format, player_name
),
ranked AS (
    SELECT
        match_format,
        player_name,
        runs_scored,
        batting_average,
        strike_rate,
        wickets_taken,
        bowling_average,
        economy_rate,
        ROUND(
            (runs_scored * 0.01) +
            (batting_average * 0.5) +
            (strike_rate * 0.3) +
            (wickets_taken * 2) +
            ((50 - bowling_average) * 0.5) +
            ((6 - economy_rate) * 2),
            2
        ) AS weighted_score,
        ROW_NUMBER() OVER (
            PARTITION BY match_format
            ORDER BY
                ROUND(
                    (runs_scored * 0.01) +
                    (batting_average * 0.5) +
                    (strike_rate * 0.3) +
                    (wickets_taken * 2) +
                    ((50 - bowling_average) * 0.5) +
                    ((6 - economy_rate) * 2),
                    2
                ) DESC
        ) AS rn
    FROM combined
)
SELECT
    match_format,
    player_name,
    runs_scored,
    batting_average,
    strike_rate,
    wickets_taken,
    bowling_average,
    economy_rate,
    weighted_score
FROM ranked
WHERE rn <= 10
ORDER BY match_format, weighted_score DESC;
""",

"22. Head-to-head match analysis in last 3 years": """
SELECT
    LEAST(sm.team1_name, sm.team2_name) AS team_a,
    GREATEST(sm.team1_name, sm.team2_name) AS team_b,
    COUNT(*) AS total_matches,
    SUM(CASE
        WHEN sm.status LIKE CONCAT(LEAST(sm.team1_name, sm.team2_name), '%won%')
        THEN 1 ELSE 0 END) AS wins_team_a,
    SUM(CASE
        WHEN sm.status LIKE CONCAT(GREATEST(sm.team1_name, sm.team2_name), '%won%')
        THEN 1 ELSE 0 END) AS wins_team_b,
    ROUND(
        100 * SUM(CASE
            WHEN sm.status LIKE CONCAT(LEAST(sm.team1_name, sm.team2_name), '%won%')
            THEN 1 ELSE 0 END) / COUNT(*), 2
    ) AS win_pct_team_a,
    ROUND(
        100 * SUM(CASE
            WHEN sm.status LIKE CONCAT(GREATEST(sm.team1_name, sm.team2_name), '%won%')
            THEN 1 ELSE 0 END) / COUNT(*), 2
    ) AS win_pct_team_b
FROM series_matches_v2 sm
WHERE sm.start_date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
  AND sm.status LIKE '%won%'
GROUP BY team_a, team_b
HAVING COUNT(*) >= 5
ORDER BY total_matches DESC;
""",

"23. Recent player form and momentum": """
WITH ranked AS (
    SELECT
        b.batsman_name,
        b.runs,
        b.strike_rate,
        sm.start_date,
        ROW_NUMBER() OVER (
            PARTITION BY b.batsman_name
            ORDER BY sm.start_date DESC, b.match_id DESC, b.innings_no DESC
        ) AS rn
    FROM batting_scorecard_v2 b
    JOIN series_matches_v2 sm
        ON sm.match_id = b.match_id
),
last10 AS (
    SELECT *
    FROM ranked
    WHERE rn <= 10
),
summary AS (
    SELECT
        batsman_name,
        COUNT(*) AS innings_in_sample,
        ROUND(AVG(CASE WHEN rn <= 5 THEN runs END), 2) AS avg_runs_last_5,
        ROUND(AVG(runs), 2) AS avg_runs_last_10,
        ROUND(AVG(CASE WHEN rn <= 5 THEN strike_rate END), 2) AS avg_sr_last_5,
        ROUND(AVG(CASE WHEN rn BETWEEN 6 AND 10 THEN strike_rate END), 2) AS avg_sr_prev_5,
        SUM(CASE WHEN runs >= 50 THEN 1 ELSE 0 END) AS scores_above_50,
        ROUND(STDDEV(runs), 2) AS runs_stddev
    FROM last10
    GROUP BY batsman_name
)
SELECT
    batsman_name,
    innings_in_sample,
    avg_runs_last_5,
    avg_runs_last_10,
    avg_sr_last_5,
    avg_sr_prev_5,
    ROUND(avg_sr_last_5 - avg_sr_prev_5, 2) AS recent_sr_trend,
    scores_above_50,
    runs_stddev,
    CASE
        WHEN avg_runs_last_5 >= 45 AND scores_above_50 >= 3 AND runs_stddev <= 20 THEN 'Excellent Form'
        WHEN avg_runs_last_5 >= 30 AND scores_above_50 >= 2 AND runs_stddev <= 28 THEN 'Good Form'
        WHEN avg_runs_last_5 >= 20 THEN 'Average Form'
        ELSE 'Poor Form'
    END AS form_category
FROM summary
WHERE innings_in_sample = 10
ORDER BY avg_runs_last_5 DESC, recent_sr_trend DESC;
""",

"24. Successful batting partnerships between consecutive batsmen": """
SELECT
    bat1_name AS Player1,
    bat2_name AS Player2,
    COUNT(*) AS partnership_count,
    ROUND(AVG(total_runs), 2) AS avg_partnership_runs,
    SUM(CASE WHEN total_runs > 50 THEN 1 ELSE 0 END) AS partnerships_above_50,
    MAX(total_runs) AS highest_partnership,
    ROUND(
        100.0 * SUM(CASE WHEN total_runs > 50 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS success_rate
FROM partnerships
WHERE ABS(bat1_position - bat2_position) = 1
GROUP BY bat1_name, bat2_name
HAVING COUNT(*) >= 5
ORDER BY success_rate DESC, avg_partnership_runs DESC, highest_partnership DESC;
""",

"25. Quarterly batting performance evolution": """
WITH q AS (
    SELECT
        b.batsman_name,
        YEAR(sm.start_date) AS yr,
        QUARTER(sm.start_date) AS qtr,
        CONCAT(YEAR(sm.start_date), '-Q', QUARTER(sm.start_date)) AS year_quarter,
        COUNT(DISTINCT b.match_id) AS matches_played,
        ROUND(AVG(b.runs), 2) AS avg_runs,
        ROUND(AVG(b.strike_rate), 2) AS avg_strike_rate
    FROM batting_scorecard_v2 b
    JOIN series_matches_v2 sm
      ON sm.match_id = b.match_id
    GROUP BY b.batsman_name, YEAR(sm.start_date), QUARTER(sm.start_date)
    HAVING COUNT(DISTINCT b.match_id) >= 3
),
t AS (
    SELECT
        q.*,
        LAG(avg_runs) OVER (PARTITION BY batsman_name ORDER BY yr, qtr) AS prev_avg_runs,
        LAG(avg_strike_rate) OVER (PARTITION BY batsman_name ORDER BY yr, qtr) AS prev_avg_strike_rate
    FROM q
),
c AS (
    SELECT
        batsman_name,
        COUNT(*) AS quarters_count
    FROM q
    GROUP BY batsman_name
    HAVING COUNT(*) >= 6
)
SELECT
    t.batsman_name,
    t.year_quarter,
    t.matches_played,
    t.avg_runs,
    t.avg_strike_rate,
    t.prev_avg_runs,
    ROUND(t.avg_runs - t.prev_avg_runs, 2) AS runs_change,
    t.prev_avg_strike_rate,
    ROUND(t.avg_strike_rate - t.prev_avg_strike_rate, 2) AS strike_rate_change,
    CASE
        WHEN t.prev_avg_runs IS NULL THEN 'Start'
        WHEN (t.avg_runs - t.prev_avg_runs) >= 5 THEN 'Improving'
        WHEN (t.avg_runs - t.prev_avg_runs) <= -5 THEN 'Declining'
        ELSE 'Stable'
    END AS quarter_trend,
    CASE
        WHEN (
            AVG(t.avg_runs) OVER (
                PARTITION BY t.batsman_name
                ORDER BY t.yr, t.qtr
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) >
            AVG(t.avg_runs) OVER (
                PARTITION BY t.batsman_name
                ORDER BY t.yr, t.qtr
                ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING
            )
        ) THEN 'Career Ascending'
        WHEN (
            AVG(t.avg_runs) OVER (
                PARTITION BY t.batsman_name
                ORDER BY t.yr, t.qtr
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) <
            AVG(t.avg_runs) OVER (
                PARTITION BY t.batsman_name
                ORDER BY t.yr, t.qtr
                ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING
            )
        ) THEN 'Career Declining'
        ELSE 'Career Stable'
    END AS career_phase
FROM t
JOIN c
  ON c.batsman_name = t.batsman_name
ORDER BY t.batsman_name, t.yr, t.qtr;
""",
}

# -------------------------------------------------
# DB CONNECTION HELPERS
# -------------------------------------------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cricket_db"
    )

def fetch_player_by_id(player_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM players WHERE player_id = %s", (player_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row

def add_player(player_id, name, nick_name, bat_style, bowl_style, role,
               birth_place, dob_text, country, intl_team, teams, image_id):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO players (
            player_id, name, nick_name, bat_style, bowl_style, role,
            birth_place, dob_text, country, intl_team, teams, image_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        player_id, name, nick_name, bat_style, bowl_style, role,
        birth_place, dob_text, country, intl_team, teams, image_id
    ))
    conn.commit()
    cursor.close()
    conn.close()

def update_player(player_id, name, nick_name, bat_style, bowl_style, role,
                  birth_place, dob_text, country, intl_team, teams, image_id):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        UPDATE players
        SET name = %s,
            nick_name = %s,
            bat_style = %s,
            bowl_style = %s,
            role = %s,
            birth_place = %s,
            dob_text = %s,
            country = %s,
            intl_team = %s,
            teams = %s,
            image_id = %s
        WHERE player_id = %s
    """
    cursor.execute(query, (
        name, nick_name, bat_style, bowl_style, role,
        birth_place, dob_text, country, intl_team, teams, image_id,
        player_id
    ))
    conn.commit()
    cursor.close()
    conn.close()

def delete_player(player_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM players WHERE player_id = %s", (player_id,))
    conn.commit()
    cursor.close()
    conn.close()


# -------------------------------------------------
# MAIN UI
# -------------------------------------------------
section = globals().get("SECTION", "SQL")

if section == "SQL":
    st.title("🏏 Cricket SQL Analytics")

    question = st.selectbox("Select SQL Question", list(queries.keys()))
    conn = get_connection()

    try:
        if queries[question] is not None:
            df = pd.read_sql(queries[question], conn)

            st.write("### Question")
            st.write(question)

            st.write("### Result")
            st.dataframe(df, hide_index=True, use_container_width=True)

        else:
            st.write("ICC Men's T20 World Cup-2026 Season- Points Table")

            groups = pd.read_sql(
                """
                SELECT DISTINCT group_name
                FROM series_points_table
                WHERE series_id = %s
                ORDER BY group_name
                """,
                conn,
                params=(SERIES_ID,)
            )["group_name"].tolist()

            tabs = st.tabs(groups)

            for i, g in enumerate(groups):
                with tabs[i]:
                    df = pd.read_sql(
                        """
                        SELECT
                            team_full_name AS Team,
                            matches_played AS 'Matches Played',
                            matches_won AS Won,
                            matches_lost AS Lost,
                            no_result AS NoResult,
                            points AS Points,
                            nrr AS NRR
                        FROM series_points_table
                        WHERE series_id=%s AND group_name=%s
                        ORDER BY matches_won DESC, points DESC
                        """,
                        conn,
                        params=(SERIES_ID, g)
                    )

                    st.subheader(g)
                    st.dataframe(df, hide_index=True, use_container_width=True)
    finally:
        conn.close()

elif section == "CRUD":
    st.title("🏏 CRUD Operations")
    st.subheader("Players")

    crud_action = st.selectbox(
        "Choose Operation",
        ["View Players", "Add Player", "Update Player", "Delete Player"]
    )

    if crud_action == "View Players":
        conn = get_connection()
        try:
            df = pd.read_sql("""
                SELECT
                    player_id,
                    name,
                    nick_name,
                    bat_style,
                    bowl_style,
                    role,
                    birth_place,
                    dob_text,
                    country,
                    intl_team,
                    teams,
                    image_id
                FROM players
                ORDER BY player_id
            """, conn)
        finally:
            conn.close()

        st.dataframe(df, hide_index=True, use_container_width=True)

    elif crud_action == "Add Player":
        with st.form("add_player_form"):
            player_id = st.number_input("Player ID", min_value=1, step=1)
            name = st.text_input("Name")
            nick_name = st.text_input("Nick Name")
            bat_style = st.text_input("Bat Style")
            bowl_style = st.text_input("Bowl Style")
            role = st.text_input("Role")
            birth_place = st.text_input("Birth Place")
            dob_text = st.text_input("DOB Text")
            country = st.text_input("Country")
            intl_team = st.text_input("International Team")
            teams = st.text_area("Teams")
            image_id = st.text_input("Image ID")

            submitted = st.form_submit_button("Add Player")

            if submitted:
                try:
                    add_player(
                        int(player_id), name, nick_name, bat_style, bowl_style, role,
                        birth_place, dob_text, country, intl_team, teams, image_id
                    )
                    st.success("Player added successfully.")
                except Exception as e:
                    st.error(f"Error adding player: {e}")

    elif crud_action == "Update Player":
        player_id = st.number_input("Enter Player ID to Update", min_value=1, step=1, key="update_player_id")

        if st.button("Load Player"):
            player = fetch_player_by_id(int(player_id))
            if player:
                st.session_state["loaded_player"] = player
            else:
                st.warning("Player not found.")

        if "loaded_player" in st.session_state:
            player = st.session_state["loaded_player"]

            with st.form("update_player_form"):
                name = st.text_input("Name", value=player.get("name", ""))
                nick_name = st.text_input("Nick Name", value=player.get("nick_name", ""))
                bat_style = st.text_input("Bat Style", value=player.get("bat_style", ""))
                bowl_style = st.text_input("Bowl Style", value=player.get("bowl_style", ""))
                role = st.text_input("Role", value=player.get("role", ""))
                birth_place = st.text_input("Birth Place", value=player.get("birth_place", ""))
                dob_text = st.text_input("DOB Text", value=player.get("dob_text", ""))
                country = st.text_input("Country", value=player.get("country", ""))
                intl_team = st.text_input("International Team", value=player.get("intl_team", ""))
                teams = st.text_area("Teams", value=player.get("teams", ""))
                image_id = st.text_input("Image ID", value=str(player.get("image_id", "")))

                submitted = st.form_submit_button("Update Player")

                if submitted:
                    try:
                        update_player(
                            int(player["player_id"]),
                            name, nick_name, bat_style, bowl_style, role,
                            birth_place, dob_text, country, intl_team, teams, image_id
                        )
                        st.success("Player updated successfully.")
                        del st.session_state["loaded_player"]
                    except Exception as e:
                        st.error(f"Error updating player: {e}")

    elif crud_action == "Delete Player":
        player_id = st.number_input("Enter Player ID to Delete", min_value=1, step=1, key="delete_player_id")

        if st.button("Show Player"):
            player = fetch_player_by_id(int(player_id))
            if player:
                st.write("### Player Details")
                st.json(player)
                st.session_state["delete_id"] = int(player_id)
            else:
                st.warning("Player not found.")

        if "delete_id" in st.session_state:
            if st.button("Confirm Delete"):
                try:
                    delete_player(st.session_state["delete_id"])
                    st.success("Player deleted successfully.")
                    del st.session_state["delete_id"]
                except Exception as e:
                    st.error(f"Error deleting player: {e}")

else:
    st.error(f"Unknown section: {section}")