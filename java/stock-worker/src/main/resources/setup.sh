#!/bin/sh
command -v python >/dev/null 2>&1 || { echo >&2 "Python not found."; exit 1; }
exit 0
