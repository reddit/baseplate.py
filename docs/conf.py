# sphinx docs config
#
# This file is execfile()d with the current directory set to its
# containing dir.
import os
import sys

import alabaster

# where to find the modules to import for autodoc
sys.path.insert(0, os.path.abspath(".."))

# -- General configuration ------------------------------------------------
# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "alabaster",
    "sphinxcontrib.spelling",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3.7", None),
    "pyramid": ("https://docs.pylonsproject.org/projects/pyramid/en/1.9-branch", None),
    "cassandra": ("https://datastax.github.io/python-driver/", None),
    "pymemcache": ("https://pymemcache.readthedocs.io/en/latest/", None),
    "kazoo": ("https://kazoo.readthedocs.io/en/latest/", None),
    "kombu": ("https://kombu.readthedocs.io/en/latest/", None),
    "redis": ("https://redis-py.readthedocs.io/en/latest/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/13/", None),
    "requests": ("https://requests.readthedocs.io/en/stable/", None),
}

# The suffix of source filenames.
source_suffix = ".rst"

# The encoding of source files.
source_encoding = "utf-8"

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "Baseplate.py"

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
language = "en"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["docs/html"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "friendly"

# -- Options for HTML output ----------------------------------------------

html_theme_path = [alabaster.get_path()]
html_static_path = ["images"]

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "alabaster"

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
# html_title =

# which templates to put in the sidebar.  we're just removing the relations
# section from the defaults here, that's "next article" and "previous article"
html_sidebars = {"**": ["about.html", "searchbox.html", "navigation.html"]}

html_theme_options = {
    "description": "Reddit's Python Service Framework",
    "github_button": False,
    "github_repo": "baseplate.py",
    "github_user": "reddit",
    "github_banner": True,
    "logo": "baseplate.png",
    "logo_name": True,
    "show_powered_by": False,
    "show_related": False,
    "show_relbars": True,
    "page_width": "960px",
}

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
html_favicon = "images/favicon.png"

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = "%b %d, %Y"

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
html_use_smartypants = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
html_show_copyright = False

# -- Autodoc --
autodoc_member_order = "bysource"

# -- Spelling --
spelling_word_list_filename = "words.txt"
spelling_show_suggestions = True
