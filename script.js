import {
  Subject,
  scan,
  interval,
  map,
  filter,
  mergeMap,
  buffer,
  tap,
  merge,
  of,
  from,
  distinctUntilKeyChanged,
  skipWhile,
} from "rxjs";

const bulkLimit = 2;
const queueLimit = 100;
const processDebounceTime = 1000;
const maxCommandsProcessedInParallel = 100;

const queueBlocker = new Subject();
const queue = new Subject();
const tokenSystem = new Subject().pipe(
  scan((total, current) => total + current, 0),
  map((value) => ({
    tokenLimitExceeded: value >= maxCommandsProcessedInParallel,
  })),
  distinctUntilKeyChanged("tokenLimitExceeded"),
  skipWhile((value) => !value.tokenLimitExceeded)
);

const debounceInterval = interval(processDebounceTime);

// importovat rxjs, vyresit typy. Implementovat, otestovat.

const events$ = merge(queue, queueBlocker, tokenSystem).pipe(
  scan(
    (state, current) => {
      if (current.data) {
        if (state.commands.length >= queueLimit) {
          console.log("queue exceeded, ignoring command", current.data);
        } else if (state.blocked || state.tokenLimitExceeded) {
          state = {
            ...state,
            commands: reorderArray([...state.commands, current]),
            commandsToSend: [],
          };
        } else {
          state = { ...state, commands: [], commandsToSend: [current] };
        }
      } else if (current.nextBulk) {
        if (state.blocked || state.tokenLimitExceeded) {
          state = {
            ...state,
            commands: reorderArray([...current.nextBulk, ...state.commands]),
            commandsToSend: [],
          };
        } else {
          state = { ...state, commands: [], commandsToSend: current.nextBulk };
        }
      } else if (current.tokenLimitExceeded || current.blocked) {
        state = { ...state, ...current, commandsToSend: [] };
      } else if (current.blocked === false) {
        state = {
          ...state,
          ...current,
          commandsToSend: state.commands,
          commands: [],
        };
      } else if (current.tokenLimitExceeded === false) {
        if (!state.blocked) {
          state = {
            ...state,
            ...current,
            commandsToSend: state.commands.slice(0, 1),
            commands: state.commands.slice(1),
          };
        } else {
          state = {
            ...state,
            ...current,
          };
        }
      }
      return state;
    },
    {
      commands: [],
      commandsToSend: [],
      blocked: false,
      tokenLimitExceeded: false,
    }
  ),
  filter((state) => !state.blocked && !state.tokenLimitExceeded),
  //pluck?
  map((state) => state.commandsToSend),
  mergeMap((commands) => from(commands)),
  tap((command) => {
    tokenSystem.next(1);
    if (command.blocking) {
      console.log("processing blocking command", command.data);
      queueBlocker.next({ blocked: true });
    }
    if (command.immediate) {
      console.log("sending immediate command", command.data);
      makeHttpRequest([command]);
    }
  }),
  filter((event) => !event.immediate),
  buffer(debounceInterval),
  filter((buffer) => !!buffer.length),
  mergeMap((buffer) => {
    const reorderedBuffer = reorderArray(buffer);
    const singleCommands = [];
    const bulkCommands = [];

    reorderedBuffer.forEach((command) => {
      if (command.noBulk) {
        singleCommands.push([command]);
      } else {
        bulkCommands.push(command);
      }
    });

    const thisBulk = bulkCommands.slice(0, bulkLimit);
    const nextBulk = bulkCommands.slice(bulkLimit);

    if (nextBulk.length) {
      if (nextBulk.find((command) => command.blocking)) {
        queueBlocker.next({ blocked: false });
      }
      tokenSystem.next(-nextBulk.length);
      queue.next({ nextBulk });
    }

    return merge(from(singleCommands), of(thisBulk));
  })
);

events$.subscribe((bulk) => {
  if (!bulk.length) {
    return;
  }

  makeHttpRequest(bulk);
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

function makeHttpRequest(commands) {
  if (!commands.length) {
    console.log("no commands");
    return;
  }

  console.log(
    `made request`,
    commands.map((command) => command.data)
  );

  setTimeout(() => {
    console.log(
      "got response",
      commands.map((command) => command?.data)
    );

    commands.forEach((command) => {
      tokenSystem.next(-1);
    });
    if (commands.some((command) => command.blocking)) {
      queueBlocker.next({ blocked: false });
    }
  }, 3000);
}

enqueue({ immediate: false, noBulk: true, data: 2, blocking: false });
enqueue({ immediate: false, noBulk: true, data: 3, blocking: false });
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
enqueue({ immediate: false, noBulk: false, data: 6, blocking: false });
enqueue({ immediate: false, noBulk: false, data: 7, blocking: true });
enqueue({ immediate: true, noBulk: true, data: 8 });

setTimeout(() => {
  enqueue({ immediate: true, noBulk: true, data: 9, isFirst: true });
  enqueue({ immediate: false, noBulk: false, data: 10 });
}, 2000);
