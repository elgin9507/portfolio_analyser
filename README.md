# Portfolio Analyser

This program accepts portfolio data from source and detects errors in given data. Errors are then exported
as configured. At this point, only Excel import and JSON file export are implemented.

## Directory Structure

- `analyser`: Contains all source code.
- `data`: Input data for the analyser.
- `results`: Results of analysis are written here.
- `requirements.txt`: Python dependencies for the project.
- `main.py`: Entrypoint for the analyser. Contains some driver code for starting the analyser.


### Source Code Structure

- `analyser/data_feeds`: Input data feeds. Currently only Excel (.xlsx) file implemented
- `analyser/exporters`: Exporter implementations for analysis results. Json file is the only implementation at this point.
- `analyser/reconcilers`: Contains "reconciler" classes which basically detect different kinds of errors and do calculations if necessary. They report back detected errors.
- `analyser/analyser.py`: Contains `PortfolioAnalyzer` class that basically connects dots together. It's a composite
data structre that depends on other parts of system (e.g. reconcilers, input streams) and runs the analysis by
employing them
- `analyser/enums.py`: Contains useful enumerations for the program.
- `analyser/errors.py`: Error definitions for different kinds of issues found in portfolio data.
- `analyser/interfaces.py`: Abstractions for different parts of the whole system.
- `analyser/utils.py`: Small useful functions which are not directly related to portfolio analysis.

## Running analyser

1. Install Python dependencies

```bash
pip install -r requirements.txt
```

2. Start the analyser

```bash
python main.py
```
