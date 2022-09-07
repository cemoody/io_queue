"""Sphinx configuration."""
project = "IOQueue"
author = "Christopher Moody"
copyright = "2022, Christopher Moody"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
