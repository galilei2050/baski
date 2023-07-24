import typing
import yaml
from pathlib import Path


YmlConsumer = typing.Callable[[dict], None]


def for_yml_file_in_dir(root: str, path: str, fn: YmlConsumer, *args, **kwargs):
    root_path = Path(root).resolve()
    if root_path.is_file():
        root_path = root_path.parent

    for file in (root_path / path).iterdir():
        data = yaml.load(file.open(), Loader=yaml.Loader)
        fn(data, *args, **kwargs)
