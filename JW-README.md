
# Todo

To do

SocketServer - handle() works on a connection basis.  In a slave, how to transition to normal client handling when we have finished the initialisation.

Moved to asyncio - borrowed ideas from <https://jcristharif.com/msgspec/examples/asyncio-kv.html>

More test_server tests (replica and master?)

# Local development environment

Set up with:

* `devbox.json` (specifies python 3.12 to be managed by `devbox`)
* `requirements.txt` (specifies python development tools to be installed with `pip`)
* `pyproject.toml` (settings for python development tools)
* `.envrc` (automatically sets up devbox on `cd`)
* `Makefile` (runner for local development)
