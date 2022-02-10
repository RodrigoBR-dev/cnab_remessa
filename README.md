# cnab_remessa

Receives and process a file, pattern cnab400, in a bucket on Google Cloud Platform.
Transform each line in a object to proceed the registration as a bankslip.
The objects are pushed to an array with at most 10k length and sended to pubsub service of GCP to a better enqueue performance.