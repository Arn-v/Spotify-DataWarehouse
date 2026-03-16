# Architecture

## System Overview

```mermaid
graph TB
    subgraph Data Sources
        KG[Kaggle CSV Dataset<br/>~114k tracks]
        API[Spotify Web API<br/>Featured • New Releases • Recs]
    end

    subgraph ETL Pipelines
        KE[KaggleCSVExtractor]
        SE[SpotifyAPIExtractor<br/>Graceful Degradation]
        TT[TrackTransformer]
        AT[ArtistTransformer]
        AFT[AudioFeaturesTransformer]
        DD[Deduplicator]
        PL[PostgresLoader<br/>Upsert]
    end

    subgraph Data Warehouse
        DT[dim_track]
        DA[dim_artist]
        DAL[dim_album]
        DG[dim_genre]
        DDT[dim_date]
        BTA[bridge_track_artist]
        BAG[bridge_artist_genre]
        FAF[fact_audio_features]
        FTP[fact_track_popularity]
    end

    subgraph Analytics
        TA[TrendingAnalyzer]
        GA[GenreAnalyzer]
        APA[AudioProfileAnalyzer<br/>K-Means]
    end

    subgraph Aggregates
        ATT[agg_trending_tracks]
        AGS[agg_genre_stats]
        AAP[agg_audio_profiles]
    end

    subgraph Presentation
        SL[Streamlit Dashboard<br/>Plotly • Dark Theme]
        JN[Jupyter Notebooks]
    end

    subgraph Observability
        PRL[pipeline_run_log]
        LOG[Structured JSON Logs]
    end

    KG --> KE
    API --> SE
    KE --> TT & AT & AFT
    SE --> DD --> TT & AT & AFT
    TT & AT & AFT --> PL
    PL --> DT & DA & DAL & DG & DDT & BTA & BAG & FAF & FTP

    FTP --> TA --> ATT
    FAF --> GA --> AGS
    FAF --> APA --> AAP

    ATT & AGS & AAP --> SL
    ATT & AGS & AAP --> JN

    PL -.-> PRL
    SE -.-> LOG
```

## Pipeline Architecture

### ETL Pattern

All pipelines follow the **Extract → Transform → Load** pattern using abstract base classes:

```
BasePipeline
├── KaggleLoadPipeline    (CSV → DW, run once)
├── IngestionPipeline     (API → DW, scheduled)
└── AnalyticsPipeline     (DW → aggregates)
```

Each pipeline uses `execute()` which wraps `run()` with lifecycle hooks:
- `_setup()` → `run()` → `_teardown()`
- On failure: `_on_failure()` is called
- Every run writes to `pipeline_run_log` for observability

### Star Schema Design

The data warehouse uses a **star schema** optimized for analytics queries:

- **Dimensions**: Track, Artist, Album, Genre, Date
- **Bridge tables**: Track↔Artist (many-to-many), Artist↔Genre (many-to-many)
- **Facts**: Audio Features (slowly changing), Track Popularity (append-only time series)
- **Aggregates**: Trending, Genre Stats, Audio Profiles (rebuilt by AnalyticsPipeline)

### Scheduling

APScheduler runs ingestion + analytics on a configurable interval (default: every 6 hours).

### Graceful Degradation

The `SpotifyAPIExtractor` tries each API endpoint independently. If one returns 403 or fails, it logs a warning and continues with the remaining endpoints.
