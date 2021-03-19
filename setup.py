import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bpcs-conduit", # Replace with your own username
    version="0.0.6",
    author="Allen Plummer",
    author_email="ahplummer@gmail.com",
    description="Python SDK for BPCS Conduit data virtualization tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BlueprintConsulting/Conduit-PythonSDK",
    project_urls={
        "Bug Tracker": "https://github.com/BlueprintConsulting/Conduit-PythonSDK/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        'requests','urllib3',
    ]
)