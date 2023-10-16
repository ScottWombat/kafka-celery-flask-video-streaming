#!/bin/sh
cd src
gunicorn --bind 0.0.0.0:5000 wsgi:app