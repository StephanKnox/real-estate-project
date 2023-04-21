from setuptools import find_packages, setup

setup(
    name="realestate_scraping",
    packages=find_packages(exclude=["realestate_scraping_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
