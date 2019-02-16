from dataclasses import dataclass


@dataclass
class Config:
    gcp_project_id: str = 'optionchan-222710'
    gcp_cs_bucket_name: str = 'jikken'
    gcp_bq_dataset_name: str = 'jikken'
    gcp_ds_kind: str = 'optionchan'
    gcp_ds_key_id: str = 'prev_future_price'
    gcp_cf_url_base: str ='https://us-east1-optionchan-222710.cloudfunctions.net'
