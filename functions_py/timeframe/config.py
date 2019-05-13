from dataclasses import dataclass


@dataclass
class Config:
    gcp_project_id: str = 'optionchan-222710'
    gcp_bq_dataset_name: str = 'optionchan'
