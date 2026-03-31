## Cricket Data Pipeline (T20 Franchise Analytics)

This project is a cricket analytics pipeline built with Python, SQLite, SQLAlchemy, and CricAPI.
It extracts match and scorecard data for major men’s T20 franchise competitions, transforms nested API responses into structured tables, and loads them into a local SQLite database.
The pipeline stores match-level, innings-level, batting, and bowling data for downstream analysis.
Reusable SQL queries are included to analyse top run scorers, wicket takers, best bowling figures, and competition coverage.
The project is designed as a compact end-to-end ETL portfolio piece.

## Tech Stack 

Python, SQLite, SQLAlchemy, REST API (CricAPI)

## Data Coverage 

- Indian Premier League (IPL 2025)
- Big Bash League (BBL 2024–25)
- Caribbean Premier League (CPL 2025)

## How To Run

```bash
export CRICKETDATA_API_KEY="your_api_key_here"
python -m src.main
```

## Example Outputs

### Matches By Competition

| Competition | Season  | Matches |
| ----------- | ------- | ------- |
| IPL         | 2025    | 75      |
| BBL         | 2024–25 | 44      |
| CPL         | 2025    | 34      |


### Best Bowling Figures (Single Match)

| Player            | Team                     | Wickets | Runs | Overs | Economy |
|------------------|--------------------------|--------|------|-------|--------|
| Nathan McAndrew  | Sydney Thunder           | 5      | 16   | 4.0   | 4.0    |
| Mark Steketee    | Melbourne Stars          | 5      | 17   | 4.0   | 4.2    |
| Imran Tahir      | Guyana Amazon Warriors   | 5      | 21   | 4.0   | 5.2    |
| Gudakesh Motie   | Guyana Amazon Warriors   | 5      | 21   | 3.2   | 6.3    |
| Mitchell Starc   | Delhi Capitals           | 5      | 35   | 3.4   | 9.5    |


### Highest Individual Scores (Single Match)

| Player            | Team                     | Runs | Balls | Strike Rate |
|------------------|--------------------------|------|-------|------------|
| Abhishek Sharma  | Sunrisers Hyderabad      | 141  | 55    | 256.36     |
| Tim Seifert      | Saint Lucia Kings        | 125  | 53    | 235.85     |
| Steven Smith     | Sydney Sixers            | 121  | 64    | 189.06     |
| Colin Munro      | Trinbago Knight Riders   | 120  | 57    | 210.53     |
| Rishabh Pant     | Lucknow Super Giants     | 118  | 61    | 193.44     |

### Top Wicket Takers

| Player                         | Team                     | Wickets |
|--------------------------------|--------------------------|---------|
| Prasidh Krishna               | Gujarat Titans           | 25      |
| Noor Ahmad                    | Chennai Super Kings      | 24      |
| Imran Tahir                   | Guyana Amazon Warriors   | 23      |
| Josh Hazlewood                | Royal Challengers Bengaluru | 22   |
| Trent Boult                   | Mumbai Indians           | 22      |
| Arshdeep Singh                | Punjab Kings             | 21      |
| Usman Tariq                   | Trinbago Knight Riders   | 20      |
| Ravisrinivasan Sai Kishore    | Gujarat Titans           | 19      |
| Gudakesh Motie                | Guyana Amazon Warriors   | 18      |
| Jasprit Bumrah                | Mumbai Indians           | 18      |

### Top Run Scorers

| Player              | Team                     | Runs |
|--------------------|--------------------------|------|
| Sai Sudharsan      | Gujarat Titans           | 738  |
| Suryakumar Yadav   | Mumbai Indians           | 717  |
| Virat Kohli        | Royal Challengers Bengaluru | 657 |
| Shubman Gill       | Gujarat Titans           | 615  |
| Shreyas Iyer       | Punjab Kings             | 604  |
| Prabhsimran Singh  | Punjab Kings             | 599  |
| Yashasvi Jaiswal   | Rajasthan Royals         | 559  |
| Mitchell Marsh     | Lucknow Super Giants     | 552  |
| Priyansh Arya      | Punjab Kings             | 545  |
| KL Rahul           | Delhi Capitals           | 539  |

