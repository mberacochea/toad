import subprocess
from datetime import datetime
from enum import Enum
from typing import Optional
import os

from sqlmodel import Field, Relationship, SQLModel

FTP_PATH = "/nfs/ftp/public/databases/metagenomics/temp/protein-db-dump-files"


class TimeStampedMixin(SQLModel):
    created_at: datetime = Field(default=datetime.utcnow(), nullable=False)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


class Assembly(TimeStampedMixin, table=True):
    accession: str = Field(primary_key=True)
    files: list["File"] = Relationship(back_populates="assembly")

    @property
    def dest_folder(self):
        return f"{FTP_PATH}/{self.accession[:6]}/{self.accession}_data"

    @property
    def tarball_path(self):
        return f"{FTP_PATH}/{self.accession[:6]}/{self.accession}"

    @property
    def ready(self):
        total = len(self.files)
        pending = sum(map(lambda x: x.status == CopyStatus.PENDING, self.files))
        completed = sum(map(lambda x: x.status == CopyStatus.COMPLETED, self.files))
        missing = sum(map(lambda x: x.status == CopyStatus.MISSING, self.files))
        error = sum(map(lambda x: x.status == CopyStatus.ERROR, self.files))

        if completed == total:
            print("Done")
            return True
        if missing == total or error == total:
            print(f"Assembly {self.accession} completly failed.")
            return False
        if pending:
            print("Files copy pending.")
            return False
        if missing:
            print("Missing files.")
            return False
        if error:
            print("Some failed")
            return False
        print("WUT?")
        return False

    def dump_metadata(self):
        pass


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

    status: CopyStatus = Field(default=CopyStatus.PENDING, nullable=False)

    copy_exitcode: int = Field(nullable=True)
    copy_stdout: str = Field(nullable=True)
    copy_stderr: str = Field(nullable=True)

    def copy(self):
        dest_file = f"{self.assembly.dest_folder}/{self.file_alias}"
        print(f"cp {self.file_path} {dest_file}")
        if not os.path.exists(self.file_path):
            self.status = CopyStatus.MISSING
        else:
            print(f"cp {self.file_path} {dest_file}")
            run_return = subprocess.run(
                f"cp {self.file_path} {dest_file}",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if run_return.returncode == 0:
                self.status = CopyStatus.COMPLETED
            else:
                self.status = CopyStatus.ERROR
            self.copy_exitcode = run_return.returncode
            self.copy_stdout = run_return.stdout
            self.copy_stderr = run_return.stderr
            print(f"{self.status}")
        return self
