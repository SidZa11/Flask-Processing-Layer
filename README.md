This is Flask APP as Processing Layer of EtaFlux.

For Run python's processing layer at multiple instances / threaded (make it multithread server)

=> Install Gunicorn: pip install gunicorn
=> Update requirements.txt with gunicorn.
=> Run the app with multiple workers:
    => gunicorn --workers 4 --bind 0.0.0.0:5000 run:app

NOTE: 
    workers 4: Runs 4 worker processes to handle requests in parallel.
    Adjust the number of workers based on your CPU cores (e.g., 2-4 workers per core is a common rule).