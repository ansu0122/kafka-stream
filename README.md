# kafka-streams
Learn about implementation of a simple kafka streaming application.

This project is aimed to setup the streaming of the browser history by means of the kafka producer and aggregating/logging the top 5 root domain out of the history with the 100 message cadence.

Clone the project.

Make sure to add the "data/" folder with the browser history csv file to the project root directory.
"Order" (integer) and "url" fields are required.

To run the setup use docker-compose and the command as follows:
> docker-compose up -d --build

The results of the root domain aggregation and the top 5 domains can be found in the consumer log file.

Shut down the setup by running
> docker-compose down
