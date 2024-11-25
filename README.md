# Reddit Decompress and Data Cleaning

This project processes Reddit data organized by month, extracting and cleaning data from submissions and comments to create focused datasets for analysis.

## Features

### Comment File

The comment file contains 21 attributes. We retained the following 6 relevant attributes:

- `id`, `link_id`, `subreddit`, `created_utc`, `body`, `score`

### Submission File

The submission file has 56 attributes. We selected these 7 key attributes:

- `id`, `subreddit`, `created_utc`, `title`, `selftext`, `permalink`, `score`

### Filtered by Popular Subreddits

The data is filtered by subreddit categories in Politics, Sports, and Economics:

#### Politics

- `politics`, `PoliticalDiscussion`, `unpopularopinion`, `Conservative`, `PoliticalHumor`

#### Sports

- `nba`, `sports`, `nfl`, `PremierLeague`, `formula1`

#### Economics

- `Economics`, `AskEconomics`, `inflation`, `economicCollapse`, `badeconomics`

The output includes three separate CSV files saved in the `decompress` directory:

- `*-politics.csv`
- `*-sports.csv`
- `*-economics.csv`

## Implementation Steps

### Prerequisites

Install the required Python package:

```bash
pip3 install zstandard
```

### Running the Script

Use the `decompress.py` script to decompress and clean Reddit data. The input can include multiple `.zst` files or directories:

```bash
python3 decompress.py *.zst
```

```bash
python3 decompress.py */
```

Ensure your input files are in `.zst` format and contain Reddit data structured by month for accurate processing.
