"""microburst — AWS failure injection proxy."""

try:
    from microburst._version import __version__
except ImportError:  # source checkout without build metadata
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("microburst")
    except PackageNotFoundError:
        __version__ = "0.0.0.dev0"
