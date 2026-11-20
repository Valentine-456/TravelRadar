from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from backend.settings import Settings, get_settings

router = APIRouter()


class FrontendConfig(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    mapbox_token: str
    mapbox_style: str


@router.get("/config", response_model=FrontendConfig, response_model_by_alias=True)
def get_frontend_config(settings: Annotated[Settings, Depends(get_settings)]) -> FrontendConfig:
    return FrontendConfig(
        mapbox_token=settings.mapbox_token,
        mapbox_style=settings.mapbox_style,
    )
