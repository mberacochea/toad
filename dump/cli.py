from typing import Optional
import csv
import os
import shutil

import typer
from sqlalchemy.future.engine import Engine
from sqlmodel import Session, SQLModel, create_engine
from typing_extensions import Annotated

from rich.progress import track

from dump.models import Assembly, CopyStatus, File

app = typer.Typer()


FTP_PATH = "/nfs/ftp/public/databases/metagenomics/temp/protein-db-dump-files"

__version__ = "0.1.0"


def version_callback(value: bool):
    if value:
        print(f"Toad Version: {__version__}")
        raise typer.Exit()


def create_database(db: str) -> Engine:
    engine: Engine = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(engine)
    return engine


@app.command(help=("Copy the files for an assembly."))
def copy(database: str, assembly_accession: str):
    engine = create_database(database)
    with Session(engine) as session:
        assembly = session.get(Assembly, assembly_accession)

        if assembly.ready:
            print(f"Assembly {assembly.accession} done")
            return

        if not os.path.exists(assembly.dest_folder):
            os.makedirs(assembly.dest_folder)

        pending_files = [f for f in assembly.files if f.status == CopyStatus.PENDING]

        for file_ in pending_files:
            file_.copy()
            session.add(file_)
            session.commit()

        session.refresh(assembly)

        shutil.make_archive(assembly.tarball_path, "gztar", assembly.dest_folder)
        shutil.rmtree(assembly.dest_folder)


@app.command(help="Add entries to the, template and entries.")
def init(
    database: str,
    files_csv: Annotated[
        str, typer.Option(help="A file with one assembly result file per line.")
    ],
):
    engine = create_database(database)

    with Session(engine) as session:
        with open(files_csv, "r") as files_csv_handler:
            csv_reader = csv.reader(files_csv_handler)
            next(csv_reader)
            assembly_files = {}
            for row in csv_reader:
                assembly_files.setdefault(row[1], []).append(row)

            for assembly_accession in track(
                assembly_files.keys(), description="Importing..."
            ):
                assembly = Assembly(accession=assembly_accession)
                session.add(assembly)
                session.commit()
                session.refresh(assembly)
                for assembly_file_row in assembly_files[assembly.accession]:
                    file_ = File(
                        assembly_accession=assembly.accession,
                        mgya=assembly_file_row[0],
                        file_path=assembly_file_row[2],
                        file_alias=assembly_file_row[3],
                        file_description=assembly_file_row[4],
                        pipeline_version=float(assembly_file_row[5]),
                    )
                    session.add(file_)
                session.commit()


@app.callback()
def common(
    ctx: typer.Context,
    version: Annotated[
        Optional[bool],
        typer.Option("--version", callback=version_callback, is_eager=True),
    ] = None,
):
    pass


if __name__ == "__main__":
    app()
