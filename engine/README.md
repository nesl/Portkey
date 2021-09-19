This folder hosts the solver and migration implementation.

First, solver.js fetches and assembles the Network Oracle and slot access traces.

It then computes a cost matrix and stores this solution in a file.

Next, a migration handler executes the migration.

Due to issues with migration (as a result of a fascinating Redis migration issue), the migration has been incorporated into a modified version of redis-cli in the source.

We will soon have an implementation that does not rely on this redis-cli modification (once the underlying Redis issue is absolved).

Example usage:

> node solver.js
