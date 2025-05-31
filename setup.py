from setuptools import setup, find_packages

setup(
    name="real-time-revenue-insights",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "streamlit",
        "kafka-python",
        "python-dotenv",
        # Add other dependencies as needed
    ],
) 