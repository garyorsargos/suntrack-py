from setuptools import setup, find_packages

setup(
    name="tts_data_client",
    version="0.1.0",
    description="Tracking-the-Sun Data Client: Query the OEDI S3 data lake for solar installation data.",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "s3fs",
        "pandas",
        "pyarrow"
    ],
    python_requires=">=3.7",
) 