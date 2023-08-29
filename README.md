# usage-accountant
A library the Sentry application uses to account for usage of shared system
resources broken down by feature.

The library is as simple as a thin wrapper around a Kafka producer. It exists
only to ensure consistency in what we produce from different system and to
ensure we cap the amount of messages per second.

We have systems that need to track shared resources usage per feature built
in different language. This repo contains all of them in separate directories.
