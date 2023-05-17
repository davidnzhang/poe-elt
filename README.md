# Path Of Exile Data Pipeline
A data pipeline with Snowflake, dbt, Airflow, Docker, Terraform, and GCP

## Description
Path of Exile is a dark fantasy multiplayer action role playing game with a focus on combat, loot, and deep character customisation. Notorious for having a high level of complexity in both its [core mechanics](https://www.pathofexile.com/passive-skill-tree) and [itemisation](https://www.reddit.com/r/pathofexile/comments/g1ksx2/what_returning_after_a_few_years_feels_like), the use of a slew of third party tools such as build planners, item filters, and trading apps is often a neccessity for most players to fully enjoy the game. Nevertheless, the game has attracted a growing fanbase of dedicated players, including streamers, [build theorists](https://www.reddit.com/r/pathofexile/comments/scgko9/ms_painta_bit_early_but_we_got_the_manifesto_and/), and casual gamers (me).

The core PVE gameplay loop consists of defeating enemies, collecting loot, and progressively improving the player's character through leveling up and equipping increasingly powerful items. The game's highly active trade system drives a dynamic in-game economy that is active year-round and revolves around the seasonal updates of its [league-based system](https://www.pathofexile.com/crucible). With over 200,000 peak concurrent players in the past month, there is a plethora of data available for the analysis of player behaviour, character builds, economy movements, and the unique interactions and synergies between a myriad of items and skills available to the player.

## Objective
The pipeline ingests daily leaderboard and economy data from a third-party API (poe.ninja) and stores this into a data lake as a daily snapshot. A daily batch job then consumes this data via external tables built on top of the staging layer, applying transformations and creating fact and dimension tables to be used by our dashboard to provide insights and analytics on player characteristics and economy movements. Metrics analysed include popular items and their prices over time, character archetypes and builds, and patterns in character attributes etc.

## Dataset
[poe.ninja](https://poe.ninja/) is a third-party website that queries the official Path of Exile ladder and trade APIs. Data is provided as a snapshot at the time of query and is available as nested json files, split by the API used. In the case of economy data this is further subdivided based on the item category in the corresponding API call.

While the [official API](https://www.pathofexile.com/developer/docs) will be able to provide a more complete picture of player data and returns it in a format more conducive to storage in a relational warehouse, it is currently restricted by rate limits therefore making full daily extracts difficult to achieve.

## Tools & Technologies
- Cloud - [Google Cloud Platform](https://cloud.google.com/)
- Data Lake - [Google Cloud Storage](https://cloud.google.com/storage)
- Infrastructure as Code  - [Terraform](https://www.terraform.io/)
- Containerisation - [Docker](https://www.docker.com/), [Docker Compose](https://docs.docker.com/compose/)
- Orchestration - [Airflow](https://airflow.apache.org/)
- Data Warehouse - [Snowflake](https://www.snowflake.com/en/)
- Transformation - [dbt](https://www.getdbt.com/)
- Data Visualisation - [Power BI](https://powerbi.microsoft.com/en-au/)
- Languages - SQL, Python

## Architecture
![architecture](https://github.com/davidnzhang/poe-elt/assets/130720014/f03e14d9-b875-4ca1-a606-bbb79a820935)

### Daily ELT Flow
- Leaderboard and in-game economy data collected from the poe.ninja API is moved to a raw google cloud storage bucket
- Raw data is preprocessed and stored in staging folders
- Staged data is transformed in dbt via external tables built on top of staging folders
- Core tables are then consumed by Power BI to gain insights on player character build patterns and economy movement over time

## Dashboard
![dashboard-class-overview-compressed](https://github.com/davidnzhang/poe-elt/assets/130720014/65584176-78a0-4204-a67c-b2099fe4c73a)
<br />  

![dashboard-economy-compressed](https://github.com/davidnzhang/poe-elt/assets/130720014/e62b8661-b6f6-49ec-931a-5f37deb1125f)
<br />  

![dashboard-delve-compressed](https://github.com/davidnzhang/poe-elt/assets/130720014/41df0388-bca2-434c-a6d6-9a78c7dc3f7c)

## Reflection
- IAM, Service Accounts, and Snowflake integrations with GCP for Snowpipe/External Tables via Pub/Sub were configured manually through the console and the gcloud CLI. The next step would be to include them in the Terraform setup.

- The poe.ninja API data is a nested json file, with metrics related to the skill and item usage etc. provided as an index to a list of character names where positions may change with each API call. Due to the nature and structure of this setup, it was not possible to map out relations between certain metrics at the character grain. An improvement would be to look into querying the official API to replace or fill out gaps in existing tables.

- Certain metrics are yet to be incorporated e.g. cluster jewels, skill gems, and metrics related to the character skill tree e.g. keystones and mastery. Due to the many interactions between items, skills, and skill gems in Path of Exile, it would be interesting to pull this data into the workflow for deeper analysis of character builds and item crafting, making use of other sources such as the official API and other third-party databases e.g. poedb
