from typing import Optional
import csv
import json
import os

import typer
from sqlalchemy.future.engine import Engine
from sqlmodel import Session, SQLModel, create_engine, select
from typing_extensions import Annotated

from rich.progress import track

from dump.models import Assembly, File

app = typer.Typer()


__version__ = "0.1.0"


def version_callback(value: bool):
    if value:
        print(f"Toad Version: {__version__}")
        raise typer.Exit()


def create_database(db: str) -> Engine:
    engine: Engine = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(engine)
    return engine


@app.command(help=("Dump all the assemblies in the db."))
def dump_all(database: str, output_folder: str):
    engine = create_database(database)
    with Session(engine) as session:
        assemblies = session.exec(select(Assembly))
        for assembly in track(
            assemblies, description="Exporting the assemblies json files"
        ):
            dump_json(database, assembly.accession, output_folder)


@app.command(help=("Dump the assembly json files."))
def dump_json(database: str, assembly_accession: str, output_folder: str):
    engine = create_database(database)
    with Session(engine) as session:
        assembly = session.get(Assembly, assembly_accession)

        if assembly is None:
            raise Exception(f"Assembly {assembly_accession} not found")

        # JSON with the description of the files #
        pipelines = set(f.pipeline_version for f in assembly.files)

        for pipeline_version in pipelines:
            files_for_pipeline: File = assembly.files_for_pipeline_version(
                pipeline_version
            )

            is_private = all(
                [
                    f.job_is_private
                    or f.sample_is_private
                    or f.study_is_private
                    or f.assembly_is_private
                    for f in files_for_pipeline
                ]
            )

            json_content = {
                "assembly": assembly.accession,
                "pipeline_version": pipeline_version,
                "is_private": is_private,
                "files": [],
            }

            for file_ in files_for_pipeline:
                json_content["files"].append(
                    {
                        "file_path": file_.file_path,
                        "description": file_.file_description,
                    }
                )
            output = f"{assembly.dest_folder(output_folder)}"
            os.makedirs(output, exist_ok=True)
            ouput_json = f"{output}/{assembly.accession}_{pipeline_version}.json"
            with open(ouput_json, "w", encoding="utf-8") as f:
                json.dump(json_content, f, indent=4)


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
                        job_is_private=row[6] == "1",
                        sample_is_private=row[7] == "1",
                        study_is_private=row[8] == "1",
                        assembly_is_private=row[9] == "1",
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
