import importlib.util
from pathlib import Path
from typing import Any


def import_script(script_file: str, method_name: str):
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
