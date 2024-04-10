# Python Azure functions

## Python Azure functions setup.

Init azure function project:

```console
$ func init
$ func init <project-name> --worker-runtime python --model V2
```

Create azure function:

```console
$ func new
$ func new --template "Http Trigger" --name MyHttpTrigger
```

Create python virtual environment:

```console
$ py -m venv .venv
```

Activate python virtual environment:

```console
$ .venv\scripts\activate
```

Add the following requirements to "requirements.txt" file:

- azure-functions
- azure-functions-durable

Install the requirements:

```console
$ python -m pip install -r requirements.txt
$ pip list
```

Start Azurite:

```console
azurite --silent --location c:\azurite --skipApiVersionCheck
```

Start the Azure functions:

```console
$ func start
```

## Python Azure functions testing setup.

Create python virtual environment:

```console
$ py -m venv .venv
```

Activate python virtual environment:

```console
$ .venv\scripts\activate
```

Install pytest library

```console
$ python -m pip install -r requirements.txt
$ pip install pytest
$ pip list
```

Execute tests

```console
$ pytest
```

Debug tests

Enable in VSCode Sidebar the Testing tool: Right click on the Sidebar > Testing

Configure Python Tests: Click on Configure Python Tests > pytest. Then, VSCode adds automatically to the the project the test debugging configuration. You can now debug your tests with VSCode.
