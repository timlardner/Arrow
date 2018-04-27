from celery_worker.celery import app


@app.task
def add(x, y):
    return x + y