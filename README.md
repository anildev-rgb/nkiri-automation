# Nkiri historical worker

This repository is a GitHub-hosted worker for the historical ThenKiri
audit queue. It downloads files on the temporary runner, uploads them directly
to VidFiles, and sends only metadata plus the finished VidFiles URLs to the
authenticated Nkiri API on `https://nkiri.vip`.

The live VPS database is never copied to or from this repository. The public
website service is not stopped by this worker. Five repositories share one
global queue: each repository runs four GitHub-hosted jobs, each job launches
four independent workers (16 workers per repository, 80 across the fleet).
The five repositories divide the queue into 40 movie shards, 20
English/other-series shards, and 20 Korean-drama shards. Each worker has its
own resumable state file, download folder, and lock port. Shard state files
are committed after each successful batch.

The source queue is audit data only. It is not imported into the site until a
post has working source links, uploads successfully, and receives a successful
API response.
