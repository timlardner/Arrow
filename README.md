# Arrow

The purpose of this repository is to provide a proof-of-concept of how Apache Arrow (or more specifically, Plasma) can be used to parallelise traditionally compute-heavy tasks such as the filtering of data frames, and to do so outside CPython's GIL.

We store large objects in Plasma's in-memory object store that can be retrieved quickly by other processes. Lightweight Celery workers pick up tasks dispached by the main process and retreive the data frames from Plasma. Here, they can perform processing without affecting any other Python process before writing their results back to Plasma. Plasma identifies objects with a 20-byte identifier - we are using the SHA1 digest of the filename and any filters in order to create a unique ID.

Ideally, the Plasma API would be better documented so that we can look at performing checks before retrieving (or attempting to retreive) objects from the cache, but this works well now as demonstrator.
