import {
  Subject,
  bindCallback,
  ReplaySubject,
  BehaviorSubject,
  scan,
  interval,
  withLatestFrom,
  map,
  filter,
  take,
  partition,
  mergeMap,
  takeUntil,
  Observable,
  combineLatest,
  mergeScan,
  combineLatestAll,
  buffer,
  bufferCount,
  delay,
  empty,
  skipUntil,
  takeWhile,
  single,
  find,
  mapTo,
  NEVER,
  switchMap,
  tap,
  debounceTime,
  delayWhen,
  timer,
  merge,
  startWith,
  of,
  from,
  bufferTime,
} from "rxjs";

const bulkLimit = 10;
const queueLimit = 100;
const processDebounceTime = 4000;
const maxCommandsProcessedInParallel = 2;

// TODO remove queueBlocker and push everything to queue?
const queueBlocker = new Subject();
const queue = new Subject();
const tokenSystem = new Subject().pipe(
  scan((total, current) => total + current, 0)
);

const debounceInterval = interval(processDebounceTime);

const events$ = merge(queue, queueBlocker).pipe(
  scan(
    (state, current) => {
      if (current.data) {
        if (state.commands.length >= queueLimit) {
          console.log("queue exceeded, ignoring command", current.data);
          return state;
        }
        if (state.blocked) {
          state = {
            ...state,
            commands: reorderArray([...state.commands, current]),
          };
        } else {
          state = { ...state, commands: [current] };
        }
        return state;
      } else if (current.commands) {
        return { ...state, commands: current.commands };
      } else if (typeof current.blocked === "boolean") {
        return { ...state, ...current };
      }
    },
    { commands: [], blocked: false }
  ),
  filter((state) => !state?.blocked),
  map((state) => {
    const commands = state.commands;

    const blockingCommandIndex = commands.findIndex(
      (command) => command.blocking
    );

    if (blockingCommandIndex > -1) {
      const commandsToBeSent = commands.slice(0, blockingCommandIndex + 1);
      const commandsAfterBlock = commands.slice(blockingCommandIndex + 1);
      console.log(
        "blocking because of",
        commandsToBeSent[commandsToBeSent.length - 1].data
      );
      queueBlocker.next({ blocked: true });

      queue.next({ commands: commandsAfterBlock });
      return commandsToBeSent;
    }
    return commands;
  }),

  mergeMap((commands) => from(commands)),
  tap((command) => {
    if (command.immediate) {
      console.log("sending immediate", command.data);
      makeHttpRequest([command]);
    }
  }),

  filter((event) => !event.immediate),
  buffer(debounceInterval),
  filter((buffer) => !!buffer.length),
  // pro kazdy buffer emituji single eventy a bulky jako jednotliva pole
  mergeMap((buffer) => {
    const reorderedBuffer = reorderArray(buffer);
    const singleCommands = [];
    const bulkCommands = [];

    reorderedBuffer.forEach((x) => {
      if (x.noBulk) {
        singleCommands.push([x]);
      } else {
        bulkCommands.push(x);
      }
    });
    return merge(from(singleCommands), of(bulkCommands));
  })
);

events$.subscribe((batch) => {
  if (!batch.length) {
    return;
  }
  console.log(
    "batch",
    batch.map((el) => el.data)
  );

  const thisBatch = batch.slice(0, bulkLimit);
  const nextBatch = batch.slice(bulkLimit);

  makeHttpRequest(thisBatch);

  if (nextBatch.length) {
    console.log(
      "next bulk",
      nextBatch.map((el) => el.data)
    );

    nextBatch.forEach((el) => {
      if (el.blocking) {
        console.log("unblocking because not sent", el.data);
        queueBlocker.next({ blocked: false });
      }
      queue.next(el);
    });
  }
});

function enqueue(command) {
  console.log(
    `enqueuing ${command.noBulk ? "single" : "bulk"} command, ${
      command.data
    }, ${command.blocking ? "blocking" : ""}, ${
      command.immediate ? "immediate" : ""
    }`
  );
  queue.next(command);
}

function getDebounceTime(immediate) {
  return immediate ? 0 : processDebounceTime;
}

function reorderArray(arr) {
  return arr.filter((el) => el.isFirst).concat(arr.filter((el) => !el.isFirst));
}

let hasFreeToken = true;

tokenSystem.subscribe((value) => {
  console.log("token system", value);
  hasFreeToken = value < maxCommandsProcessedInParallel;
});

function makeHttpRequest(commands) {
  if (!commands.length) {
    console.log("no commands");
    return;
  }

  commands = commands
    .map((command) => {
      if (!hasFreeToken) {
        console.log(
          "max commands in parallel exceeded, sending commands back",
          command.data
        );
        if (command.blocking) {
          console.log("unblocked");
          queueBlocker.next({ blocked: false });
        }
        queue.next(command);
        return;
      }
      tokenSystem.next(1);
      return command;
    })
    .filter((command) => !!command);

  console.log(
    `made request`,
    commands.map((command) => command.data)
  );

  setTimeout(() => {
    console.log(
      "got response",
      commands.map((command) => command?.data)
    );

    commands.forEach(() => tokenSystem.next(-1));
    if (commands.some((command) => command.blocking)) {
      console.log("unblocked");
      queueBlocker.next({ blocked: false });
    }
  }, 3000);
}

enqueue({ immediate: false, noBulk: true, data: 2, blocking: false });
enqueue({ immediate: true, noBulk: true, data: 3, blocking: false });
// add blocking
enqueue({
  immediate: false,
  noBulk: false,
  data: 4,
  blocking: false,
  isFirst: false,
});
enqueue({
  immediate: false,
  noBulk: false,
  data: 5,
  blocking: false,
  isFirst: true,
});
enqueue({ immediate: false, noBulk: false, data: 6, blocking: true });
enqueue({ immediate: false, noBulk: true, data: 7 });
enqueue({ immediate: true, noBulk: true, data: 8 });

setTimeout(() => {
  enqueue({ immediate: true, noBulk: true, data: 9, isFirst: true });
  enqueue({ immediate: false, noBulk: false, data: 10 });
}, 2000);
