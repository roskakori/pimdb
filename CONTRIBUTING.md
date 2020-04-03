# Contributing to pimdb

## Project setup

The following steps describe how to setup a local developer environment for
pimdb.

1. Create a virtual environment and activate it:
   ```bash
   $ python -m venv venv
   $ . venv/bin/activate
   ```
1. Install the required packages:
   ```bash
   $ pip install --upgrade pip
   $ pip install -r dev-requirements.txt
   ```
1. Activate the pre-commit hook:
   ```bash
   $ pre-commit install
   ```

## Testing

To run the test suite:

```bash
$ pytest
```

## Coding guidelines

To validate most of the guidelines and automatically fix minor deviations, run:

```bash
$ pre-commit run --all-files
```
