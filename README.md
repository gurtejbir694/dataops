# DataOps CLI

A generic DataOps CLI for data quality pipelines, supporting SQLite, PostgreSQL, Airflow, and Streamlit.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/dataops-project.git
   cd dataops-project
   ```

2. **Install uv** (if not installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Set up virtual environment**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Linux/Mac
   # Or for Nushell: overlay use .venv/bin/activate.nu
   ```

4. **Install dependencies**:
   ```bash
   uv sync
   uv pip install .
   ```

5. **Optional: Install Airflow/PostgreSQL/Streamlit support**:
   ```bash
   uv pip install .[airflow,postgres,dashboard]
   ```

## Configuration

1. **Set up config.yaml**:
   ```bash
   cp config.yaml.example .dataops/config.yaml
   ```
   Edit `.dataops/config.yaml` for SQLite or PostgreSQL.

2. **Set up environment variables** (optional, for PostgreSQL/Airflow)**:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your credentials.

3. **Set up quality checks** (for custom schemas):
   ```bash
   cp quality_checks.yaml.example quality_checks.yaml
   ```
   Edit `quality_checks.yaml` to define your fields and checks.

## Usage

### Using Existing Data

Check quality on a custom database table or CSV with a user-defined schema:

- **Database**:
   ```bash
   dataops check-quality --source db --table-name your_table \
     --checks-config quality_checks.yaml --verbose
   ```

- **CSV**:
   ```bash
   dataops check-quality --source csv --csv-path /path/to/data.csv \
     --checks-config quality_checks.yaml --verbose
   ```

Example `quality_checks.yaml` for a school database:
```yaml
fields:
  - name: rollno
    type: string
    checks:
      not_null: true
      unique: true
  - name: phone
    type: string
    checks:
      regex: "^\\+?[1-9]\\d{1,14}$"
  - name: results
    type: float
    checks:
      range: [0, 100]
  - name: grade
    type: string
    checks:
      not_null: true
```

### Generating Synthetic Data (Optional)

Generate synthetic data for testing:
```bash
dataops init --verbose
dataops generate --n 100 --verbose
```

Run quality checks on synthetic data:
```bash
dataops check-quality --source db --verbose
dataops check-quality --source csv --csv-path .dataops/data/sample.csv --verbose
```

View logs:
```bash
dataops logs --file data_quality --verbose
```

Check service status:
```bash
dataops status --verbose
```
## License

MIT License
