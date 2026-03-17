---
name: add-dashboard-page
description: Use this skill when creating a new Streamlit dashboard page, adding a visualization, or extending the dashboard. Triggers on "new page", "add page", "dashboard page", "new chart", "visualize".
---

# Add Dashboard Page

## Step 1: Plan the page

Before writing code, determine:
- What data does this page query? (Which tables/aggregates?)
- What charts are needed? (Line, bar, scatter, heatmap, KPI cards?)
- Any filters or interactivity? (Date range, dropdowns, sliders?)

## Step 2: Add chart builders

Add reusable Plotly chart functions to `dashboard/components/charts.py`:

```python
def create_<chart_name>(df):
    """Brief description."""
    fig = go.Figure(...)
    fig.update_layout(...)  # Match existing chart styling
    return fig
```

Keep charts consistent with existing ones — same color palette, font sizes, layout margins.

## Step 3: Create the page

Create `dashboard/pages/<page_name>.py`:

```python
import streamlit as st
from spotify_dw.db.session import SessionFactory
from dashboard.components.charts import create_<chart_name>

@st.cache_data(ttl=3600)
def load_data():
    with SessionFactory.get_session() as session:
        # Query here
        pass

def render():
    st.title("Page Title")
    data = load_data()
    st.plotly_chart(create_<chart_name>(data), use_container_width=True)

if __name__ == "__main__":
    render()
```

## Step 4: Register in app.py

Add the new page to the navigation/routing in `dashboard/app.py`.

## Step 5: Test

```bash
python -m streamlit run dashboard/app.py --server.headless true
```

Verify: page loads without errors, charts render, filters work, caching doesn't break on refresh.