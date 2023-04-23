from setuptools import find_packages, setup

setup(
    name="realestate_scraping",
    packages=find_packages(exclude=["realestate_scraping_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagit",
        "pandas",
        "selenium",
        "beautifulsoup4",
        "minio",
        "requests",
        "ratelimit"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
