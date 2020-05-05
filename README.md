# Microservice communication demo

## How to run:
+ install RabbitMQ and run it on default params (localhost, guest:guest)
+ (OPTIONAL) Install postgreSQL server. Create 2 schemas in `postgres` database: `notification_microservice` and `reservation_microservice`. Create accounts: `reservation_microservice:password` and `notification_microservice:password`
+ Install `requirements.txt` by using command `pip install requirements.txt`
+ Run (multiple instances) of `notification_service.py` by running command `python notification_service.py` (in multiple terminals)
+ Run one instance of `reservation_service.py` (command `python reservation_service.py`)

## What happens

Notificaiton service has 2 tables- `reservations` and `restaurants`. It listens to events that inform about updates in those tables. 
The notification service creates 4 entries in `restaurants` table (each instance will clear the table and repopulate it) but leaves 'reservations' table blank

Reservation service has the same tables, but it is the owner of `reservations` table.
It populates the `restaurants` table and then the `reservations` table.
Adding records to `reservations` table causes update events to fire.
Since notification service listens to updates in `reservations` table, it should automatically fill the table with the same values (when multiple instances are deployed, they should take turns populating the table)
