# TXRRC Project

TXRRC is a data pipeline and API that collects, processes, and serves drilling permit data from the Texas Railroad Commission.

# Overview

The project downloads fixed-width data files, parses them into structured tables, stores them in a database, and exposes the data through an API. The goal is to make public drilling data easier to use for analysis, research, and visualization.

# Main Features

Automated file download using Selenium

Parsing of fixed-width DAT files into structured tables

SQLite and DuckDB database support

FastAPI backend for live API access

Data refresh and zip file management

Organized folder layout for clear workflows


# How It Works

The system downloads the latest RRC permit files

Each DAT file is parsed into separate record tables

Tables are stored in a database for fast queries

The FastAPI server provides endpoints to view or update data

Running the API

Run the main API server using Python:
python backend/src/main.py

# Future Plans

Add user authentication and API keys

Add a task scheduler for automatic updates

Add data visualizations using Streamlit or a frontend app