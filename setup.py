from setuptools import setup, find_packages

setup(
    name="steam-reviews-analysis",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "streamlit",
        "pyspark",
        "pandas",
        "plotly",
        "torch",
        "transformers"
    ]
) 