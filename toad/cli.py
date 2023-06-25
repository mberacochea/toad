import time
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy.future.engine import Engine
from sqlmodel import Session, SQLModel, create_engine, func, select
from sqlmodel.engine.result import ScalarResult
from typing_extensions import Annotated

from toad.importer import import_script
from toad.models import Entry, Task, TaskStatus, Template

app = typer.Typer()


__version__ = "0.1.0"

def version_callback(value: bool):
    if value:
        print(f"Toad Version: {__version__}")
        raise typer.Exit()

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


@app.command()
def update_template(database: str, template: str):
    """Update any given template, the template file name is going to be used to identify it"""
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
    """Run a batch of tasks"""
    engine = create_database(database)
    with Session(engine) as session:
        tasks: ScalarResult[Task] = session.exec(
            select(Task).where(Task.status == TaskStatus.PENDING).limit(batch_size)
        )
        for task in tasks:
            task.run()
            session.add(task)
            session.commit()


@app.command(help="Run the check script on the running tasks.")
def check(
    database: str,
    check_script: Annotated[
        str, typer.Option(help="Python script to check if tasks were completed.")
    ],
):
    """Run the check script on the running tasks."""
    engine = create_database(database)
    check_method: Callable = import_script(check_script, method_name="check")

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
def summary(
    database: str,
    format: Annotated[
        SummaryType, typer.Option(help="Fancy or good'ol tsv.")
    ] = SummaryType.table,
):
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


@app.command(
    help=(
        "Run continuously, checking running tasks "
        "and triggering new ones until there are no more pending tasks."
    )
)
def daemon(database: str,
    check_script: Annotated[
        str, typer.Option("--check", "-c", help="The check script path.")
    ],
    max_tasks: Annotated[
        int, typer.Option("--max-tasks", "-m", help="Max number of 'running' tasks.")
    ] = 25,
    frequency: Annotated[
        int, typer.Option("--frequency", "-f", help="Check frequency in minutes.")
    ] = 30
    ):
    engine = create_database(database)
    while True:
        print("Starting.")
        print("Checking any running tasks.")
        # Update the running jobs #
        check(database, check_script)

        with Session(engine) as session:
            # Get the number of currently running tasks
            # FIXME: use a count
            running_count: int = len(
                session.exec(
                    select(Task).where(Task.status == TaskStatus.RUNNING)
                ).all()
            )

            assert running_count >= 0

            print(f"There are {running_count} tasks running.")

            # Calculate the number of available slots for new jobs
            available_slots = max_tasks - running_count

            if available_slots > 0:
                print(f"There is room for {available_slots} more.")

                pending_tasks: list[Task] = session.exec(
                    select(Task)
                    .where(Task.status == TaskStatus.PENDING)
                    .limit(available_slots)
                )

                for pending_task in pending_tasks:
                    pending_task.run()
                    session.add(pending_task)
                    print(f"Task {pending_task.id}:{pending_task.entry.name} running.")
                session.commit()

                # Get the count of remaining pending tasks
                # FIXME: use a count
                pending_count = len(
                    session.exec(
                        select(Task).where(Task.status == TaskStatus.PENDING)
                    ).all()
                )

                assert pending_count >= 0

                # Exit if there are no more pending tasks
                if pending_count == 0:
                    print("No more tasks to run.")
                    break

        time.sleep(60 * frequency)  # Sleep for {frequency} minutes


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

@app.callback()
def common(
    ctx: typer.Context,
    version: Annotated[
        Optional[bool],
        typer.Option("--version", callback=version_callback, is_eager=True),
    ] = None,
):
    raise typer.Exit()


if __name__ == "__main__":
    app()
