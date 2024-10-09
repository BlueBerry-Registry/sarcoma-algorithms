from os import path
from codecs import open
from setuptools import setup, find_packages

# we're using a README.md, if you do not have this in your folder, simply
# replace this with a string.
here = path.abspath(path.dirname(__file__))
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Here you specify the meta-data of your package. The `name` argument is
# needed in some other steps.
setup(
    name="ohdsi-update-csv",
    version="1.0.0",
    description="This will extract a dataset from a cohort in the OMOP database and overwrites the CSV attached to the vantage6-node. The user can determine which database to overwrite.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # TODO add a url to your github repository here (or remove this line if
    # you do not want to make your source code public)
    # url='https://github.com/....',
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "vantage6-algorithm-tools",
        "pandas",
        "rpy2",
        "numpy==1.26.4",
        "ohdsi-common",
        "ohdsi-database-connector",
        "ohdsi-sqlrender",
    ],
    package_data={"v6-ohdsi-update-csv": ["sql/*.sql"]},
)
