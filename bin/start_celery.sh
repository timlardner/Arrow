#!/bin/bash

celery -A celery_worker worker --loglevel WARNING