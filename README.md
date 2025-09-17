# 2026 Summer Internship Pipeline

An automated pipeline for fetching, cleaning, and analyzing summer internship opportunities using Crew.ai agents.

## 🚀 Overview

This project automates the process of discovering and analyzing internship opportunities by:

- **Fetching** internship data from job board APIs (RapidAPI Internships)
- **Cleaning** and deduplicating job postings
- **Detecting** potentially fake job postings
- **Enriching** company information
- **Analyzing** data with pandas for insights and trends

## 📁 Project Structure

```
2026-summer-internship/
│
├── agents/                  # Crew.ai agents
│   ├── fetcher.py           # Fetches internships from Job Board API
│   ├── cleaner.py           # Dedup + filtering + fake detection
│   └── __init__.py
│
├── tools/                   # Non-agent helper code
│   ├── api_client.py        # Wraps RapidAPI Internships API
│   ├── db.py                # SQLite/Postgres setup + upsert logic
│   ├── dedup.py             # Hashing / duplicate detection utils
│   ├── enrich.py            # Company enrichment (LLM + Wikipedia)
│   └── __init__.py
│
├── data/
│   ├── raw/                 # Raw API JSON dumps (timestamped)
│   ├── processed/           # Deduped + filtered JSON
│   └── reports/             # Daily CSV/Markdown reports
│
├── configs/
│   ├── settings.yaml        # API keys, rate limits, job keywords
│   ├── prompts.yaml         # LLM prompts (fake detection, enrichment)
│   └── crew.yaml            # Crew.ai agent definitions
│
├── notebooks/               # For experiments (EDA, LLM prompt testing)
├── tests/                   # Unit tests
├── main.py                  # Orchestration (run the pipeline)
├── requirements.txt
└── README.md
```

## 🛠️ Setup

### Prerequisites

- Python 3.8+
- API key for RapidAPI Internships (or other job board APIs)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd 2026-summer-internship
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure the application:**
   ```bash
   # Edit configs/settings.yaml with your API keys and preferences
   # The API key is already configured for RapidAPI Internships
   ```

5. **Initialize the database:**
   ```bash
   python -c "from tools.db import DatabaseManager; DatabaseManager()"
   ```

## 🚀 Usage

### Daily Pipeline

Run the complete daily pipeline to fetch, clean, and report on internships:

```bash
python main.py --mode daily
```

### Weekly Analysis

Generate a weekly summary and trend analysis:

```bash
python main.py --mode weekly
```

### Data Export

Export processed data to CSV:

```bash
# Export all data
python main.py --mode export

# Export with custom filename
python main.py --mode export --export-filename "my_internships.csv"

# Export data from specific date range
python main.py --mode export --start-date "2025-01-01" --end-date "2025-01-31"
```

### Data Summary

View a summary of all processed data:

```bash
python main.py --mode summary
```

### Custom Search

Search for specific internships:

```bash
# Search for security internships
python main.py --mode custom --security-keywords "cybersecurity" "security analyst" --location "London" --limit 100

# Search for general internships
python main.py --mode custom --internship-keywords "software engineering" "data science" --location "San Francisco" --limit 100

# Search for both types
python main.py --mode custom --internship-keywords "intern" --security-keywords "cyber" --location "New York" --limit 50
```

### Configuration

Edit `configs/settings.yaml` to customize:

- **Search keywords** and locations
- **API credentials** and rate limits
- **Database settings** (SQLite or PostgreSQL)
- **Processing parameters** (deduplication, fake detection)
- **Report settings** and output formats

## 🤖 Agents

### Fetcher Agent
- Fetches raw internship data from job board APIs
- Handles rate limiting and pagination
- Stores raw data with timestamps

### Cleaner Agent
- Removes duplicate job postings
- Filters irrelevant positions
- Detects potentially fake job postings
- Enriches company information

### Data Analysis
- Analyzes processed data using pandas
- Provides statistical summaries and insights
- Exports data to CSV format
- Generates trend analysis and market insights

## 🔧 Tools

### API Client (`tools/api_client.py`)
- Wraps RapidAPI Internships API with error handling
- Implements rate limiting and connection management
- Normalizes data from different sources
- Handles salary parsing and company information extraction

### Database Manager (`tools/db.py`)
- Handles SQLite/PostgreSQL operations
- Stores raw and processed data
- Provides querying capabilities

### Deduplication Manager (`tools/dedup.py`)
- Identifies duplicate job postings
- Uses fuzzy matching algorithms
- Configurable similarity thresholds

### Company Enricher (`tools/enrich.py`)
- Enriches company information using Wikipedia
- Integrates with LLM for additional insights
- Caches enrichment data

## 📊 Data Flow

1. **Fetch**: Raw job data is retrieved from APIs and stored
2. **Clean**: Data is deduplicated, filtered, and verified
3. **Enrich**: Company information is enhanced with external data
4. **Report**: Processed data is formatted into reports

## 📊 Data Analysis

### Data Summary
- Total opportunities found
- Unique organizations and locations
- Job type distribution
- Top hiring companies
- Geographic distribution
- Industry analysis
- LinkedIn organization insights

### Export Options
- CSV export with all job data
- Date-filtered exports
- Custom filename support
- Comprehensive data fields

### Statistical Insights
- Job posting trends over time
- Company size distribution
- Industry breakdown
- Location analysis
- Remote work statistics

## 🧪 Testing

Run the test suite:

```bash
pytest tests/
```

Run with coverage:

```bash
pytest --cov=. tests/
```

## 🔍 Development

### Adding New Agents

1. Create a new agent class in `agents/`
2. Add it to `agents/__init__.py`
3. Update `configs/crew.yaml` with agent configuration
4. Add tasks to the workflow

### Adding New Tools

1. Create a new tool class in `tools/`
2. Add it to `tools/__init__.py`
3. Update agent configurations to use the new tool

### Customizing Prompts

Edit `configs/prompts.yaml` to modify LLM prompts for:
- Fake job detection
- Company enrichment
- Job categorization
- Report generation

## 📝 Configuration

### Settings (`configs/settings.yaml`)

Key configuration options:

```yaml
api:
  fantastic_jobs:
    api_key: "your_rapidapi_key_here"
    base_url: "internships-api.p.rapidapi.com"
    rate_limit:
      requests_per_minute: 60

search:
  keywords:
    - "intern"
    - "internship"
    - "summer intern"
  locations:
    - "San Francisco"
    - "New York"
    - "Remote"

processing:
  deduplication:
    similarity_threshold: 0.8
  fake_detection:
    enable_llm_verification: true
```

### Crew Configuration (`configs/crew.yaml`)

Defines agent roles, goals, and workflow:

```yaml
fetcher_agent:
  role: "Data Fetcher"
  goal: "Fetch internship data from job board APIs"
  tools: ["api_client", "db_manager"]
```

## 🚨 Troubleshooting

### Common Issues

1. **API Rate Limiting**: Adjust rate limits in `settings.yaml`
2. **Database Errors**: Check database permissions and connection
3. **Memory Issues**: Reduce batch sizes in processing
4. **LLM Errors**: Verify API keys and model availability

### Logs

Check `logs/pipeline.log` for detailed error information.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [Crew.ai](https://www.crewai.com/) for the agent framework
- [RapidAPI Internships](https://rapidapi.com/hub) for the job board API
- The open-source community for various tools and libraries

## 📞 Support

For questions or issues:

1. Check the troubleshooting section
2. Review the logs
3. Open an issue on GitHub
4. Contact the maintainers

---

**Happy job hunting! 🎯**
