import subprocess
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Optional

from jinja2 import Template as Jinja2Template
from sqlmodel import Field, Relationship, SQLModel


class TimeStampedMixin(SQLModel):
    created_at: datetime = Field(default=datetime.utcnow(), nullable=False)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


class Entry(TimeStampedMixin, table=True):
    """Entry represents an any a piece of data to be used
    with a template for the execution of a task"""

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    tasks: list["Task"] = Relationship(back_populates="entry")


class Template(TimeStampedMixin, table=True):
    """This is a Jinja2 template to be used for the exeuction of an Entry"""

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    content: str

    tasks: list["Task"] = Relationship(back_populates="template")


class TaskStatus(str, Enum):
    """Task Status"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


@lru_cache
def load_template(template_string: str) -> Jinja2Template:
    return Jinja2Template(template_string)


class Task(TimeStampedMixin, table=True):
    """This model represents the exeuction of an Entry with a given Tempalte"""

    id: Optional[int] = Field(default=None, primary_key=True)

    entry_id: int = Field(foreign_key="entry.id", nullable=False)
    entry: Entry = Relationship(back_populates="tasks")

    template_id: int = Field(foreign_key="template.id", nullable=False)
    template: Template = Relationship(back_populates="tasks")

    status: TaskStatus = Field(default=TaskStatus.PENDING, nullable=False)
    task_launch_exitcode: int = Field(nullable=True)
    task_launch_stdout: str = Field(nullable=True)
    task_launch_stderr: str = Field(nullable=True)

    task_execution_exitcode: int = Field(nullable=True)
    task_execution_stdout: str = Field(nullable=True)
    task_execution_stderr: str = Field(nullable=True)

    def run(self):
        task_template = load_template(self.template.content)
        run_return = subprocess.run(
            task_template.render(entry=self.entry),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if run_return.returncode == 0:
            self.status = TaskStatus.RUNNING
        else:
            self.status = TaskStatus.ERROR
        self.task_launch_exitcode = run_return.returncode
        self.task_launch_stdout = run_return.stdout
        self.task_launch_stderr = run_return.stderr
