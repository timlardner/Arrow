from celery import Celery

app = Celery('celery_worker',
             broker='amqp://guest@localhost//',
             backend='redis://localhost:6379/0',
             include=['celery_worker.filter']
             )

app.conf.update(
        CELERY_TASK_SERIALIZER = 'json',
        CELERY_RESULT_SERIALIZER = 'json',
        CELERY_ACCEPT_CONTENT=['json'],
        CELERY_TIMEZONE = 'Europe/London',
        CELERY_ENABLE_UTC = True
                )