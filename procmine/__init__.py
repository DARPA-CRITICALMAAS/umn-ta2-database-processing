from importlib.metadata import version
from fusemine._procmine import ProcMine

__version__ = version('procmine')

__all__ = [
    "ProcMine",
]