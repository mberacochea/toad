import importlib.util
import subprocess
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import typer
from jinja2 import Template as Jinja2Template
from rich.console import Console
from rich.table import Table
from sqlalchemy.future.engine import Engine
from sqlmodel import Session, SQLModel, create_engine, func, select
from sqlmodel.engine.result import ScalarResult
from typing_extensions import Annotated

from toad.models import Entry, Task, TaskStatus, Template

app = typer.Typer()


def create_database(db: str) -> Engine:
    """
    Create the database and tables.

    Args:
        db: The SQLlite db path
    Returns:
        The db Engine

    """
    engine: Engine = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(engine)
    return engine


def _import_script(script_file: str, method_name: str):
    script_path = Path(script_file).resolve()

    if script_path.exists():
        module_name = script_path.stem
        spec: Any = importlib.util.spec_from_file_location(module_name, script_path)
        external_module: Any = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_module)

        method = getattr(external_module, method_name, None)
        if callable(method):
            return method
        else:
            print("The specified script does not contain a callable function.")
    else:
        print("The specified script file does not exist.")


@lru_cache
def _load_template(template_string: str) -> Template:
    return Jinja2Template(template_string)


@app.command()
def update_template(database: str, template: str):
    engine = create_database(database)
    with Session(engine) as session:
        with open(template, "r") as tfile:
            query = session.exec(
                select(Template).where(Template.name == Path(template).name)
            )
            db_template = query.one()
            db_template.content = tfile.read()
            session.add(db_template)
            session.commit()
            print("Done")
            session.refresh(db_template)
            print(db_template.content)


@app.command()
def run(database: str, batch_size: int = 10):
    engine = create_database(database)
    with Session(engine) as session:
        tasks: ScalarResult[Task] = session.exec(
            select(Task).where(Task.status == TaskStatus.PENDING).limit(batch_size)
        )
        for task in tasks:
            jinja_template = _load_template(task.template.content)
            run_return = subprocess.run(
                jinja_template.render(entry=task.entry),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if run_return.returncode == 0:
                task.status = TaskStatus.RUNNING
            else:
                task.status = TaskStatus.ERROR
            task.task_launch_exitcode = run_return.returncode
            task.task_launch_stdout = run_return.stdout
            task.task_launch_stderr = run_return.stderr
            session.add(task)
            session.commit()


@app.command(help="Run the check script on the running tasks.")
def check(
    database: str,
    check_script: Annotated[
        str, typer.Option(help="Python script to check if tasks were completed.")
    ],
):
    engine = create_database(database)
    check_method: Callable = _import_script(check_script, method_name="check")

    with Session(engine) as session:
        tasks: ScalarResult[Task] = session.exec(
            select(Task).where(Task.status == TaskStatus.RUNNING)
        )
        for task in tasks:
            is_running, exit_code, stdout, stderr = check_method(
                task.entry.name,
                task.task_launch_exitcode,
                task.task_launch_stdout,
                task.task_launch_stderr,
            )
            if is_running:
                continue
            if exit_code == 0:
                task.status = TaskStatus.COMPLETED
            else:
                task.status = TaskStatus.ERROR
            task.task_execution_exitcode = exit_code
            task.task_execution_stdout = stdout
            task.task_execution_stderr = stderr
            session.add(task)
            session.commit()


class SummaryType(str, Enum):
    table = "table"
    tsv = "tsv"


@app.command(help="Get a summary of the number of tasks per status")
def summary(database: str, format: SummaryType = SummaryType.table):
    """Get a summary of the number of tasks per status"""
    engine = create_database(database)
    with Session(engine) as session:
        query = select(Task.status, func.count()).group_by(Task.status)
        result = session.exec(query).fetchall()

        table = Table(title="Task Status")

        table.add_column("Status", justify="center")
        table.add_column("Count", justify="left")

        if format == SummaryType.table:
            console = Console()
            for status, count in result:
                table.add_row(str(status), str(count))
            console.print(table)
        else:
            for status, count in result:
                print(f"{status}\t{count}")


def empty_list() -> list:
    return []


@app.command(help="Add entries to the, template and entries.")
def init(
    database: str,
    template: Annotated[str, typer.Option(help="Path to the jijna2 template.")],
    entries: Annotated[
        list[str],
        typer.Option(
            default_factory=empty_list, help="The entries to store in the DB."
        ),
    ],
    entries_file: Annotated[str, typer.Option(help="A file with one entry per line.")],
):
    # TODO: template and entries should be optional
    engine = create_database(database)

    with Session(engine) as session:
        db_template = None
        with open(template, "r") as tfile:
            db_template = Template(name=Path(template).name, content=tfile.read())
            session.add(db_template)
            session.commit()
            session.refresh(db_template)

        if entries_file and Path(entries_file).exists():
            with open(entries_file, "r") as efiles:
                for line_entry in efiles:
                    line_entry = line_entry.strip()
                    if line_entry:
                        entries.append(line_entry.strip())

        for entry in entries:
            entry = entry.strip()

            db_entry = Entry(name=entry)
            session.add(db_entry)
            session.commit()
            session.refresh(db_entry)

            task = Task(entry_id=db_entry.id, template_id=db_template.id)
            session.add(task)
            session.commit()


if __name__ == "__main__":
    app()
