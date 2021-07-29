"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="glue-launcher",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="A Lambda which prepares and starts AWS Glue jobs once dependencies are met",
    long_description="A Lambda which prepares and starts AWS Glue jobs once dependencies are met",
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["glue_launcher=glue_launcher_lambda:handler"]},
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["argparse"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
