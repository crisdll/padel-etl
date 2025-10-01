# 📊 ETL Pádel Vallès

Extract, Transform, and Load (ETL) system for padel competition data from **Lliga de Pádel del Vallès**. This project automates data extraction from a web API and stores it in a database for further analysis. Scheduled every day with GitHub Actions


## 🚀 Technologies Used

### **Backend & ETL**
- **Python 3.13** - Main language
- **pandas** - Data manipulation and analysis
- **requests** - REST API consumption
- **psycopg2** - PostgreSQL driver for Python
- **logging** - Robust logging system

### **Database**
- **Supabase** - Backend-as-a-Service platform
- **PostgreSQL** - Relational database (Supabase engine)
- **SQL** - Data queries and management

### **API & Web Scraping**
- **REST API** - Pádel Vallès League endpoints
- **JSON** - Data exchange format
- **Base64** - Parameter encoding

## 📁 Project Structure

```
padel_etl/
├── main.py                 # Main entry point
├── config.py              # API and DB configuration
├── logger_config.py       # Logging system configuration
├── etl/
│   ├── api_client.py      # API consumption client
│   ├── loaders.py         # Data loading to Supabase
│   └── utils.py           # Utilities and transformations
└── logs/
    └── padel_etl_*.log    # Log files with timestamp
```

## 🗄️ Data Model

### **Categories**
- `categoria_api_id` (PK) - Unique category ID
- `nombre` - Category name
- `genero` - Gender (Masculino/Femenino)
- `fecha` - Update date

### **Clubs**
- `id` (PK) - Auto-incremental ID
- `nombre` - Club name
- `categoria_api_id` (FK) - Reference to categories
- `fecha` - Update date

### **Matches (Enfrentamientos)**
- `enfrentamiento_api_id` (PK) - Unique match ID
- `fecha_partido` - Match date and time
- `resultado` - Match result
- `club_local_id` - Home club
- `club_visitante_id` - Away club
- `jornada` - Match day number
- `categoria_api_id` (FK) - Reference to categories

### **Results (Resultados)**
- `partido_api_id` (PK) - Unique game ID
- `enfrentamiento_api_id` (FK) - Reference to matches
- `is_local_ganador` - Indicates if home team won
- `nombre1_local`, `nombre2_local` - Home players
- `nombre1_visitante`, `nombre2_visitante` - Away players
- `set1_local`, `set1_visitante` - Set 1 score
- `set2_local`, `set2_visitante` - Set 2 score
- `set3_local`, `set3_visitante` - Set 3 score
- `pista` - Court number
- `puntos` - Game points

## ⚙️ Configuration

### **1. System Requirements**
```bash
# Install dependencies
pip install pandas psycopg2-binary requests
```

### **2. Environment Variables**
Create `config.py` file:
```python
# Supabase (PostgreSQL) configuration
USER = "your_username"
PASSWORD = "your_password"
HOST = "your_host.supabase.co"
PORT = "5432"
DBNAME = "postgres"

# API configuration
API_BASE_URL = "https://api.padelvalles.com/"
API_HEADERS = {
    'Content-Type': 'application/json; charset=utf-8'
}
API_COOKIES = {
    'session_id': 'your_session_id'
}
```

### **3. Database Schema**
Execute in Supabase:
```sql
-- Create required tables
CREATE TABLE categorias (
    categoria_api_id INTEGER PRIMARY KEY,
    nombre VARCHAR(100),
    genero VARCHAR(20),
    fecha DATE
);

CREATE TABLE clubs (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100),
    categoria_api_id INTEGER REFERENCES categorias(categoria_api_id),
    fecha DATE
);

CREATE TABLE enfrentamientos (
    enfrentamiento_api_id INTEGER PRIMARY KEY,
    fecha_partido TIMESTAMP,
    resultado VARCHAR(50),
    club_local_id VARCHAR(100),
    club_visitante_id VARCHAR(100),
    jornada INTEGER,
    fecha DATE,
    categoria_api_id INTEGER REFERENCES categorias(categoria_api_id)
);

CREATE TABLE resultados (
    partido_api_id INTEGER PRIMARY KEY,
    enfrentamiento_api_id INTEGER REFERENCES enfrentamientos(enfrentamiento_api_id),
    is_local_ganador BOOLEAN,
    nombre1_local VARCHAR(100),
    nombre2_local VARCHAR(100),
    nombre1_visitante VARCHAR(100),
    nombre2_visitante VARCHAR(100),
    set1_local INTEGER,
    set1_visitante INTEGER,
    set2_local INTEGER,
    set2_visitante INTEGER,
    set3_local INTEGER,
    set3_visitante INTEGER,
    pista VARCHAR(20),
    puntos INTEGER,
    fecha DATE
);
```

## 🚀 Execution

### **Run Complete ETL**
```bash
python main.py
```


## 📋 ETL Process

### **1. Extract**
- Data extraction from Pádel Win API
- Authentication and parameter handling
- JSON response processing

### **2. Transform**
- HTML string cleaning
- Data type conversion
- Valid record filtering
- Date normalization

### **3. Load**
- Insert/update to PostgreSQL
- Constraint and duplicate handling
- Atomic transactions
- Detailed logging

## 📊 Key Features

### **🔄 Smart Upsert**
- `ON CONFLICT` to avoid duplicates
- Automatic update of existing records
- Referential integrity preservation

### **📝 Robust Logging System**
- File logs with unique timestamp
- Level separation (DEBUG, INFO, ERROR)
- DB-independent persistence

### **🛡️ Error Handling**
- Automatic rollback on error
- Data validation before insert
- Detailed failure logging

### **⚡ Performance Optimization**
- DB connection only during load
- Batch processing
- Early filtering of invalid data

## 📈 Performance Metrics

| Metric | Value |
|---------|-------|
| **Total time** | ~2 minutes |
| **Categories processed** | 9 |
| **Clubs processed** | 77 |
| **Matches** | 294 |
| **Results** | 882 |
| **API requests** | ~300 |

## 🔍 Monitoring and Logs

Logs are stored in `logs/padel_etl_YYYYMMDD_HHMMSS.log` with detailed information:
- Precise timestamps
- Function and line of code
- Process progress
- Errors and warnings
- Final statistics


## 👥 Author

**Cristina Diéguez** - *Initial development*
---
