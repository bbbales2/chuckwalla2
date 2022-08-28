import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt") as fh:
    requirements = fh.read().splitlines()

setuptools.setup(
    name="chuckwalla2",
    version="0.0.1",
    description="The chuckwalla2 data warehouse project",
    long_description=long_description,
    url="https://github.com/bbbales2/chuckwalla2",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPLv3 License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    install_requires=requirements,
    zip_safe=False,
)
