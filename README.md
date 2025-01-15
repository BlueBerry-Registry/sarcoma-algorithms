# Blueberry Algorithm Containers

This repository contains the source code for two algorithm containers. One algorithm
is responsible for the data extraction and storing the data in a parquet file. The other
algorithm contains all analytics tools (e.g. summary, Kaplan-Meier, etc.).

## Build the Images

To build the images, run the following command:

```bash
make analytics
make session
```

To also push the images to the registry, run the following command:

```bash
make analytics PUSH=true
make session PUSH=true
```