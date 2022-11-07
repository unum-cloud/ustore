# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Unum · UKV'
copyright = '2022, Unum'
author = 'Unum'
release = open('../../VERSION', 'r').read()

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['breathe', 'm2r2']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '*.md']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_logo = '../icons/logo.png'
html_theme = 'furo'
html_static_path = ['_static']
html_css_files = [
    'custom.css'
]
html_js_files = [
    'custom.js'
]



breathe_projects = {"UKV": "../../build/xml"}
breathe_default_project = "UKV"
