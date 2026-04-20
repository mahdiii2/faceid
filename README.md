# Employee Hours Transformer Web App

A simple one-page Flask website.

## Features
- Upload Excel file
- Transform attendance data using your rules
- Download a result Excel file immediately

## Required columns
- Employee ID
- Name
- Event Sub Type
- Time

## Run locally

```bash
pip install -r requirements.txt
python app.py
```

Then open:

```text
http://127.0.0.1:5000
```

## Output sheets
- Daily Summary
- Clustered Events
- Raw Access Auth
- Rules Used
