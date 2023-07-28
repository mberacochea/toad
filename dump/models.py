from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, Relationship, SQLModel


class TimeStampedMixin(SQLModel):
    created_at: datetime = Field(default=datetime.utcnow(), nullable=False)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


class Assembly(TimeStampedMixin, table=True):
    accession: str = Field(primary_key=True)
    files: list["File"] = Relationship(back_populates="assembly")

    def dest_folder(self, output_folder):
        return f"{output_folder}/{self.accession[:6]}/{self.accession}"

    @property
    def highest_pipeline_files(self):
        pipelines = set(f.pipeline_version for f in self.files)
        largest_pipeline_version = sorted(pipelines, reverse=True)[0]
        return [f for f in self.files if f.pipeline_version == largest_pipeline_version]

    def files_for_pipeline_version(self, pipeline_version):
        return [f for f in self.files if f.pipeline_version == pipeline_version]


class CopyStatus(str, Enum):
    """COPY Status"""

    PENDING = "pending"
    MISSING = "file_missing"
    COMPLETED = "completed"
    ERROR = "error"


class File(TimeStampedMixin, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    assembly_accession: str = Field(foreign_key="assembly.accession", nullable=False)
    assembly: Assembly = Relationship(back_populates="files")

    mgya: str = Field(nullable=False)
    file_path: str = Field(nullable=False)
    file_alias: str = Field(nullable=False)
    file_description: str = Field(nullable=False)
    pipeline_version: float = Field(nullable=False)
    job_is_private: bool = Field(nullable=False)
    sample_is_private: bool = Field(nullable=False)
    study_is_private: bool = Field(nullable=False)
    assembly_is_private: bool = Field(nullable=False)
