# Stock Picker Web Application

Django web application for visualizing Minervini stock analysis results.

## Features

- **Dashboard** - Summary statistics and top performers
- **Paginated Stock List** - Browse all analyzed stocks with filters
- **Multiple Filters**:
  - All Stocks
  - Passing Minervini Criteria
  - VCP Patterns Detected
  - Stage 2 Stocks
  - Best Setups (Stage 2 + Minervini + VCP)
- **Sortable Columns** - Sort by RS, VCP score, price, symbol
- **Color-Coded Indicators** - Visual feedback for strength ratings
- **Responsive Design** - Works on desktop and mobile

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- Django 6.0+
- django-tables2 (table rendering)
- django-bootstrap5 (styling)
- tablib (export support)
- psycopg2-binary (PostgreSQL)

### 2. Run Migrations (optional, using existing DB)

The app connects to your existing PostgreSQL database. No migrations needed since we use `managed=False` models.

### 3. Start Development Server

```bash
python manage.py runserver
```

### 4. Access the Web App

Open your browser to: **http://localhost:8000**

## URL Structure

| URL | Description |
|-----|-------------|
| `/` | Dashboard with statistics and top stocks |
| `/stocks/` | Full paginated list of all stocks |
| `/stocks/?filter=passing` | Stocks passing Minervini criteria |
| `/stocks/?filter=vcp` | Stocks with VCP patterns |
| `/stocks/?filter=stage2` | Stage 2 (Advancing) stocks |
| `/stocks/?filter=stage2_vcp` | Best setups (Stage 2 + VCP + Passing) |
| `/stocks/AAPL/` | Detail page for a specific stock (e.g., AAPL) |

### Stock Detail Page Features:
- **Embedded Robinhood chart** - Live stock chart and trading view
- **Current metrics** - Price, Stage, RS, Minervini status
- **VCP analysis** - Full VCP breakdown with score and pivot price
- **Moving averages** - All MAs with trend indicators
- **Criteria checklist** - Visual breakdown of all 9 Minervini criteria
- **Historical data** - Last 60 days of metrics in a table
- **Clickable symbols** - Click any symbol in the main table to see details

## Sorting

Add `&sort=` parameter to sort:
- `?sort=-relative_strength` - Highest RS first (default)
- `?sort=-vcp_score` - Highest VCP score first
- `?sort=symbol` - Alphabetical
- `?sort=-close_price` - Price high to low

## Configuration

Database connection settings in `stock_viewer/settings.py`:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'stocks',
        'USER': 'postgres',
        'PASSWORD': 'adamesk',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
```

## Color Coding

### Relative Strength (RS)
- 🟢 Green Badge: RS ≥ 80 (Excellent)
- 🔵 Blue Badge: RS 70-79 (Strong)
- 🟡 Yellow Badge: RS 60-69 (Good)
- ⚪ Gray Badge: RS < 60 (Weak)

### VCP Score
- 🟢 Green: 70+ (High quality setup)
- 🔵 Blue: 60-69 (Good setup)
- 🟡 Yellow: 50-59 (Emerging pattern)
- ⚪ Gray: < 50 (Not detected)

### Stage
- 🔵 Blue: Stage 1 (Basing)
- 🟢 Green: Stage 2 (Advancing) - **IDEAL BUY ZONE**
- 🟡 Yellow: Stage 3 (Topping)
- 🔴 Red: Stage 4 (Declining)

## Admin Interface

Access Django admin at: **http://localhost:8000/admin**

Create superuser:
```bash
python manage.py createsuperuser
```

## Production Deployment

For production, update `stock_viewer/settings.py`:

1. Set `DEBUG = False`
2. Update `ALLOWED_HOSTS`
3. Configure static files with `collectstatic`
4. Use a production server (gunicorn, uwsgi)
5. Set up proper database credentials

Example production command:
```bash
gunicorn stock_viewer.wsgi:application --bind 0.0.0.0:8000
```
