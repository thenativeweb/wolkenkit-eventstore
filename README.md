# wolkenkit-eventstore

wolkenkit-eventstore is an open-source eventstore for Node.js that is used by wolkenkit.

## Installation

```shell
$ npm install wolkenkit-eventstore
```

## Quick start

To use wolkenkit-eventstore first you need to add a reference to your application. You also need to specify which database to use:

```javascript
const eventstore = require('wolkenkit-eventstore/postgres');
```

The following table lists all currently supported databases:

Database               | Package
-----------------------|--------------------
PostgreSQL             | `wolkenkit-eventstore/postgres`
SQL Server             | `wolkenkit-eventstore/sqlserver`
MariaDB                | `wolkenkit-eventstore/mariadb`
MySQL                  | `wolkenkit-eventstore/mysql`
MongoDB (experimental) | `wolkenkit-eventstore/mongodb`
In-memory              | `wolkenkit-eventstore/inmemory`

Once you have created a reference, you need to initialize the instance by running the `initialize` function. Hand over the connection string to your database as well as a namespace:

```javascript
await eventstore.initialize({ url: '...', namespace: 'myApp' });
```

For the in-memory database there is no need to hand over the connection string and the namespace, so in this case you only need the following call:

```javascript
await eventstore.initialize();
```

To handle getting disconnected from the database, subscribe to the `disconnect` event. Since wolkenkit-eventstore does not necessarily try to reconnect, it's probably best to restart your application:

```javascript
eventstore.on('disconnect', () => {
  // ...
});
```

Please note that since the in-memory database does not make use of an external data source it does not support the `disconnect` event.

To manually disconnect from the database call the `destroy` function:

```javascript
await eventstore.destroy();
```

If you are using the in-memory database, calling `destroy` clears all events and snapshots.

### Reading an event stream

To read the event stream for a given aggregate use the `getEventStream` function and provide the id of the aggregate. The function returns a regular Node.js readable stream:

```javascript
const aggregateId = 'd3152c91-190d-40e6-bf13-91dd76d5f374';

const eventStream = await eventstore.getEventStream(aggregateId);
```

To limit the number of events returned you may use the `fromRevision` and `toRevision` options:

```javascript
const aggregateId = 'd3152c91-190d-40e6-bf13-91dd76d5f374';

const eventStream = await eventstore.getEventStream(aggregateId, {
  fromRevision: 23,
  toRevision: 42
});
```

### Reading the last event

To read the last event for a given aggregate use the `getLastEvent` function and provide the id of the aggregate:

```javascript
const aggregateId = 'a6d18fc4-0ce3-4e3c-af43-fd451f58c0f1';

const event = await eventstore.getLastEvent(aggregateId);
```

### Saving events

To save events use the `saveEvents` function and hand over an array of events you want to save. To create the events use the `Event` constructor function of the [commands-events](https://github.com/thenativeweb/commands-events) module, and add a `revision` property to the event's `metadata` property:

```javascript
const eventStarted = new Event(...);
const eventJoined = new Event(...);

eventStarted.metadata.revision = 1;
eventJoined.metadata.revision = 2;

const savedEvents = await eventstore.saveEvents({
  events: [ eventStarted, eventJoined ]
});
```

*Please note that the `revision` starts at `1`, not – as you may expect – at `0`.*

The assignment from the given events to their appropriate aggregates is done using the events' information. The `revision` of the events *must* be given in their `metadata` section.

*Please note that the events are saved within a single atomic transaction. If saving fails for at least one event, the entire transaction is rolled back, so no events are saved at all.*

#### Saving a single event

If you only want to save a single event you may omit the brackets of the array and directly specify the event as parameter:

```javascript
const eventStarted = new Event(...);

eventStarted.metadata.revision = 1;

const savedEvents = await eventstore.saveEvents({ events: eventStarted });
```

#### Respecting the event stream order

Every event is automatically assigned a position within the entire event stream. This way you can figure out the insertion order at a later point in time, e.g. when replaying. This value is a `number` and stored using the property `event.metadata.position`.

The position is available when fetching the event stream using the `getEventStream` function, or after having called `saveEvents` on the saved events that are returned.

*Please note that the event store guarantees that the positions are monotonically increasing, but they may have gaps, i.e. some positions may be missing. This may happen, e.g. when saving events failed and had to be retried.*

### Reading unpublished events

To read all unpublished events as a stream, call the `getUnpublishedEventStream` function. The function returns a regular Node.js readable stream:

```javascript
const eventStream = await eventstore.getUnpublishedEventStream();
```

### Marking events as published

Once you have published events using an event publisher you may mark them as published using the `markEventsAsPublished` function. Hand over the aggregate id as well as the revision from which to which you would like to mark the events as published:

```javascript
const aggregateId = 'd3152c91-190d-40e6-bf13-91dd76d5f374';

await eventstore.markEventsAsPublished({
  aggregateId,
  fromRevision: 23,
  toRevision: 42
});
```

### Reading a snapshot

To read the event stream for a given aggregate use the `getSnapshot` function and provide the id of the aggregate. The function always returns the `revision` and `state` of the newest snapshot. If no snapshot exists, the function returns `undefined`:

```javascript
const aggregateId = 'd3152c91-190d-40e6-bf13-91dd76d5f374';

const snapshot = await eventstore.getSnapshot(aggregateId);
```

### Saving a snapshot

To save a snapshot for an aggregate use the `saveSnapshot` function and hand over the aggregate id, the revision and the state of the aggregate:

```javascript
const aggregateId = 'd3152c91-190d-40e6-bf13-91dd76d5f374';
const revision = 23;
const state = {
  // ...
};

await eventstore.saveSnapshot({ aggregateId, revision, state });
```

*Please note that if a snapshot was already saved for the given aggregate id and revision, this function does nothing.*

### Getting a replay

To get a replay of events use the `getReplay` function. The function returns a regular Node.js readable stream:

```javascript
const replayStream = await eventstore.getReplay();
```

To limit the number of events returned you may use the `fromPosition` and `toPosition` options:

```javascript
const replayStream = await eventstore.getReplay({
  fromPosition: 7,
  toPosition: 23
});
```

## Running the build

To build this module use [roboter](https://www.npmjs.com/package/roboter).

```shell
$ npx roboter
```

Please note that wolkenkit-eventstore uses the [Microsoft SQL Server on Linux Docker image](https://hub.docker.com/r/microsoft/mssql-server-linux/) to run SQL Server for the tests. To run this image you need to assign at least 3.25 GByte of RAM to Docker for Mac or Docker for Windows.

## License

Copyright (c) 2015-2018 the native web.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see [GNU Licenses](http://www.gnu.org/licenses/).
