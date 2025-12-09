# Marathon Qualifying Times

Work in progress. Hobby project for marathon qualifying details, by age, gender and location.

## Key features

- Scrape qualifying standards for a variety of marathons.
- Store race metadata and qualifying tables in an Azure SQL Database.
- Compute margin between a personal best and the qualifying standard.

## Prerequisites

- Python 3.11.
- Database in Azure to store data scraped from the project.
- Configuration details on the database in Azure.
- Setup with the correct firewall configurations for Azure database.

## Configuration

### 1. Create a virtual environment

```bash
python3.11 -m venv venv
```

### 2. Activate virtual environment on macOS

```bash
source venv/bin/activate
```

### 3. Install the required packages

```bash
pip install -r requirements.txt
```

### 4. Copy the `.env.template` file and rename it to `.env`

```bash
cp .env.template .env
```

### 5. Open the file and fill in the required values as instructed

### 6. Run main script

```bash
python3.11 main.py
```

### Ideas for Future Enhancements

- Add functionality for change tracking, to pull and alert on changes in the qualifying time for a given age group.

### Acknowledgements

This project is inspired by the educational notebooks and scraping techniques demonstrated by **databayes** and demoes at SQLBits 2025. References are found below:

- The **BeautifulSoup** repository, which provided foundational guidance on using Beautiful Soup for web scraping: [BeautifulSoup](https://github.com/databayes/BeautifulSoup)
- The **OpenAI** repository, which demonstrated scraping marathon result data for analysis: [OpenAI](https://github.com/databayes/OpenAI)
