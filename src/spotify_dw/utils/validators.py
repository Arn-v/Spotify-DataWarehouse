"""Pydantic models for data validation at system boundaries."""

from pydantic import BaseModel, Field, field_validator


class TrackValidator(BaseModel):
    """Validates a single track record before loading."""

    spotify_track_id: str = Field(min_length=1, max_length=62)
    track_name: str = Field(min_length=1, max_length=500)
    duration_seconds: float = Field(ge=0)
    explicit: bool = False

    @field_validator("track_name")
    @classmethod
    def normalize_name(cls, v: str) -> str:
        return v.strip().lower()


class AudioFeaturesValidator(BaseModel):
    """Validates audio feature values are within expected ranges."""

    danceability: float = Field(ge=0.0, le=1.0)
    energy: float = Field(ge=0.0, le=1.0)
    speechiness: float = Field(ge=0.0, le=1.0)
    acousticness: float = Field(ge=0.0, le=1.0)
    instrumentalness: float = Field(ge=0.0, le=1.0)
    liveness: float = Field(ge=0.0, le=1.0)
    valence: float = Field(ge=0.0, le=1.0)
    loudness: float = Field(ge=-60.0, le=10.0)
    tempo: float = Field(ge=0.0, le=300.0)
    key: int = Field(ge=0, le=11)
    mode: int = Field(ge=0, le=1)
    time_signature: int = Field(ge=1, le=7)
