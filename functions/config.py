from dataclasses import dataclass, field
from datetime import datetime
from pytz import timezone

from dataclasses_json import dataclass_json

# GCP info
GCP_PROJECT_ID = 'optionchan-222710'
GCS_BUCKET_NAME = 'jikken'
BQ_DATASET_NAME = 'jikken'
GCD_TYPE = 'optionchan'
GCD_KEY_ID = 'prev_future_price'


@dataclass_json
@dataclass
class Config:
    gcp_project_id: str = field(init=False, default=GCP_PROJECT_ID)
    gcp_cs_bucket_name: str = field(init=False, default=GCS_BUCKET_NAME)
    gcp_bq_dataset_name: str = field(init=False, default=BQ_DATASET_NAME)
    gcp_ds_kind: str = field(init=False, default=GCD_TYPE)
    gcp_ds_key_id: str = field(init=False, default=GCD_KEY_ID)
